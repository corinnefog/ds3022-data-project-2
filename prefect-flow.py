# prefect flow goes here
import boto3
import requests
import time
from prefect import flow, task

uvaid = "qfr4cu"
api_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"
submit_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

sqs = boto3.client('sqs')

@task
def trigger_api(url):
    try:
        response = requests.post(url)
        response.raise_for_status()
        payload = response.json()
        print(f"API connection triggered: {payload}")
    
        my_queue_url = payload.get('sqs_url')
        print(f"My queue URL: {my_queue_url}")
        return my_queue_url
        
    except Exception as e:
        print(f"Error triggering API: {e}")
        raise e

@task
def get_queue_attributes(queue_url):
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
        
        visible = int(attributes.get('ApproximateNumberOfMessages', 0))
        not_visible = int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
        
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

@task
def get_messages(queue_url, expected_count=21):
    all_messages = []
    
    while len(all_messages) < expected_count:
        queue_stats = get_queue_attributes.fn(queue_url)
        
        # Break if queue is empty but we already got some messages
        if queue_stats['total'] == 0 and all_messages:
            print(f"Queue empty. Collected {len(all_messages)} messages.")
            break
        
        # Wait if there are delayed/invisible messages
        if queue_stats['visible'] == 0 and queue_stats['total'] > 0:
            print(f"Waiting for delayed messages... (Collected: {len(all_messages)}/{expected_count})")
            time.sleep(5)
            continue
        
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
            
            for message in messages:
                try:
                    receipt_handle = message['ReceiptHandle']
                    attrs = message.get('MessageAttributes', {})
                    
                    order_no = attrs['order_no']['StringValue']
                    word = attrs['word']['StringValue']
                    
                    all_messages.append({'order_no': order_no, 'word': word})
                    print(f"  Parsed: order_no={order_no}, word={word}")
                    
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
    return all_messages

def order_no(message):
    return int(message['order_no'])

@task
def reorder_messages(messages):
    try:
        sorted_messages = sorted(messages, key=order_no)
        
        order_sequence = [msg['order_no'] for msg in sorted_messages]
        print(f"Order sequence: {order_sequence}")
        
        return sorted_messages
        
    except Exception as e:
        print(f"Error reordering messages: {e}")
        raise e

@task
def create_phrase(sorted_messages):
    try:
        words = [msg.get('word', '') for msg in sorted_messages]
        phrase = ' '.join(words)
        
        print(f"Created phrase from {len(words)} words: {phrase}")
        return phrase
        
    except Exception as e:
        print(f"Error creating phrase: {e}")
        raise e

@task
def send_solution(uvaid, phrase, platform):
    try:
        message_body = "Solution Message from Prefect Flow"
        
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
        print(f"HTTP Status: {response['ResponseMetadata']['HTTPStatusCode']}")
        
        return response
        
    except Exception as e:
        print(f"Error sending solution: {e}")
        raise e

@flow
def prefect_pipeline():
    my_queue_url = trigger_api(api_url)
    time.sleep(10)
    queue_stats = get_queue_attributes(my_queue_url)
    
    if queue_stats["total"] == 0:
        print("No messages to process. Exiting flow.")
        return
    
    messages = get_messages(my_queue_url, expected_count=21)
    
    if not messages:
        print("No messages collected. Exiting.")
        return
    sorted_messages = reorder_messages(messages)
    quote = create_phrase(sorted_messages)
    send_solution(uvaid, quote, "prefect")
    print(f"Final Phrase: {quote}")
   
if __name__ == "__main__":
    prefect_pipeline()
