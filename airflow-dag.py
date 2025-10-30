import boto3
import requests
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


uvaid = "qfr4cu"
api_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"
submit_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

sqs = boto3.client('sqs', region_name='us-east-1')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def trigger_api(url=api_url, **kwargs):
    try:
        response = requests.post(url)
        response.raise_for_status()
        payload = response.json()
        print(f"API connection triggered: {payload}")
        
        my_queue_url = payload.get('sqs_url')
        print(f"My queue URL: {my_queue_url}")
        kwargs['ti'].xcom_push(key='queue_url', value=my_queue_url)
        return my_queue_url
        
    except Exception as e:
        print(f"Error triggering API: {e}")
        raise e


def get_queue_attributes(**kwargs):
    ti = kwargs['ti']
    queue_url = ti.xcom_pull(task_ids='trigger_api_task', key='queue_url')
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
	)
        attributes = response.get('Attributes', {})
        
        #Know where the messages are in the queue
        visible = int(attributes.get('ApproximateNumberOfMessages', 0))
        not_visible = int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
        
        #Calculate total messages
        total = visible + not_visible + delayed
        
        print(f"Visible: {visible}, Not Visible: {not_visible}, Delayed: {delayed}, Total: {total}")
        
        return {
            'visible': visible,
            'not_visible': not_visible,
            'delayed': delayed,
            'total': total
        }
        
    except Exception as e:
        print(f"Error getting queue attributes: {e}")
        raise e


def get_messages(expected_count=21, **kwargs):
    ti = kwargs['ti']
    queue_url = ti.xcom_pull(task_ids='trigger_api_task', key='queue_url')
    all_messages = []
    
    #keep getting messages until we have the expected count
    while len(all_messages) < expected_count:
        queue_stats = get_queue_attributes(ti=kwargs['ti'])
        
        # Break if queue is empty but we already got some messages
        if queue_stats['total'] == 0 and all_messages:
            print(f"Queue empty. Collected {len(all_messages)} messages.")
            break
        
        # Wait if there are delayed/invisible messages
        if queue_stats['visible'] == 0 and queue_stats['total'] > 0:
            print(f"Waiting for delayed messages... (Collected: {len(all_messages)}/{expected_count})")
            time.sleep(5)
            continue
        
        # Poll for messages
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=5
            )
            
            messages = response.get('Messages', [])
            if not messages:
                print("No messages in this poll, waiting...")
                time.sleep(2)
                continue
            
            print(f"Received {len(messages)} messages")
            
            # Process each message
            for message in messages:
                try:
                    receipt_handle = message['ReceiptHandle']
                    attrs = message.get('MessageAttributes', {})
                    
                    #Extract needed number and word
                    order_no = attrs['order_no']['StringValue']
                    word = attrs['word']['StringValue']
                    
                    all_messages.append({'order_no': order_no, 'word': word})
                    print(f"  Parsed: order_no={order_no}, word={word}")
                    
                    #Delete message from queue
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    
                except Exception as e:
                    print(f"Error processing message: {e}")
                    
        except Exception as e:
            print(f"Error getting messages: {e}")
            time.sleep(2)
    
    print(f"Total messages collected: {len(all_messages)}")
    ti.xcom_push(key='messages', value=all_messages)

# Helper function to extract order_no as integer
def order_no(message):
    return int(message['order_no'])


def reorder_messages(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='get_messages_task', key='messages')
    try:
        sorted_messages = sorted(messages, key=order_no)

        # For debugging, print the order sequence
        order_sequence = [msg['order_no'] for msg in sorted_messages]
        print(f"Order sequence: {order_sequence}")
        
        ti.xcom_push(key='sorted_messages', value=sorted_messages)
        return sorted_messages
        
    except Exception as e:
        print(f"Error reordering messages: {e}")
        raise e



def create_phrase(**kwargs):
    ti = kwargs['ti']
    sorted_messages = ti.xcom_pull(task_ids='reorder_messages_task', key='sorted_messages')
    try:
        # Concatenate words in order to form the phrase
        words = [msg.get('word', '') for msg in sorted_messages]
        phrase = ' '.join(words)
        
        print(f"Created phrase from {len(words)} words: {phrase}")
        ti.xcom_push(key='phrase', value=phrase)
        return phrase
        
    except Exception as e:
        print(f"Error creating phrase: {e}")
        raise e


def send_solution(**kwargs):
    ti = kwargs['ti']
    phrase = ti.xcom_pull(task_ids='create_phrase_task', key='phrase')
    platform = "airflow"
    try:
        message_body = "Solution Message from Airflow DAG"
        
        # Send the message to the submission queue
        response = sqs.send_message(
            QueueUrl=submit_queue_url,
            MessageBody=message_body,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
	)
        
        print(f"Submission Response: {response}")
        # Log the HTTP status code
        print(f"HTTP Status: {response['ResponseMetadata']['HTTPStatusCode']}")
        
        return response
        
    except Exception as e:
        print(f"Error sending solution: {e}")
        raise e


with DAG(
    'sqs_puzzle_solver',
    default_args=default_args,
    description='Solve SQS puzzle by collecting and reassembling messages',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['sqs', 'puzzle', 'dp2'],
)as dag:


    trigger_api_task = PythonOperator(
        task_id='trigger_api_task',
        python_callable=trigger_api
    )
    
    # Task 3: Check queue attributes
    check_queue_task = PythonOperator(
        task_id='check_queue_task',
        python_callable=get_queue_attributes
    )
    
    # Task 4: Get all messages
    get_messages_task = PythonOperator(
        task_id='get_messages_task',
        python_callable=get_messages
    )
    
    # Task 5: Reorder messages
    reorder_messages_task = PythonOperator(
        task_id='reorder_messages_task',
        python_callable=reorder_messages
    )
    
    # Task 6: Create phrase
    create_phrase_task = PythonOperator(
        task_id='create_phrase_task',
        python_callable=create_phrase
    )
    
    # Task 7: Send solution
    send_solution_task = PythonOperator(
        task_id='send_solution_task',
        python_callable=send_solution
    )

    trigger_api_task >> check_queue_task >> get_messages_task >> reorder_messages_task >> create_phrase_task >> send_solution_task

