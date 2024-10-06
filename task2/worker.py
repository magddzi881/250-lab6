import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)

# Callback function that simulates a task
def callback(ch, method, properties, body):
    task = body.decode()
    print(f" [x] Received {task}")
    
    time_to_work = task.count('.')
    time.sleep(time_to_work) 
    print(f" [x] Done working on '{task}'")

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

print(' [*] Waiting for tasks')
channel.start_consuming()
