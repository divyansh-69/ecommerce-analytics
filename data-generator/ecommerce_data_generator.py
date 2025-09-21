import json
import random
import time
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
import uuid

fake = Faker()

class EcommerceDataGenerator:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Product categories and sample products
        self.categories = {
            'Electronics': ['iPhone 14', 'Samsung TV', 'MacBook Pro', 'iPad', 'AirPods'],
            'Clothing': ['Nike Shoes', 'Adidas Jacket', 'Levis Jeans', 'H&M Shirt', 'Zara Dress'],
            'Books': ['Python Guide', 'Data Science Handbook', 'Web Development', 'AI Basics'],
            'Sports': ['Tennis Racket', 'Football', 'Running Shoes', 'Gym Equipment'],
            'Home': ['Kitchen Set', 'Bedroom Decor', 'Living Room Sofa', 'Dining Table']
        }
        
        self.event_types = ['page_view', 'product_click', 'add_to_cart', 'purchase', 'search']
        self.event_weights = [40, 25, 15, 5, 15]  # Weighted probabilities
        
    def generate_customer_event(self):
        customer_id = f"cust_{random.randint(1000, 9999)}"
        session_id = str(uuid.uuid4())
        
        category = random.choice(list(self.categories.keys()))
        product = random.choice(self.categories[category])
        
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'customer_id': customer_id,
            'session_id': session_id,
            'event_type': random.choices(self.event_types, weights=self.event_weights)[0],
            'product_id': f"prod_{hash(product) % 10000}",
            'product_name': product,
            'category': category,
            'price': round(random.uniform(10, 1000), 2),
            'quantity': random.randint(1, 5),
            'user_agent': fake.user_agent(),
            'ip_address': fake.ipv4(),
            'location': {
                'city': fake.city(),
                'country': fake.country(),
                'latitude': float(fake.latitude()),
                'longitude': float(fake.longitude())
            }
        }
        
        return event
    
    def start_streaming(self, events_per_second=10):
        print(f"Starting data generation: {events_per_second} events/second")
        
        while True:
            try:
                # Generate batch of events
                for _ in range(events_per_second):
                    event = self.generate_customer_event()
                    
                    # Send to appropriate Kafka topic based on event type
                    if event['event_type'] == 'purchase':
                        topic = 'ecommerce-transactions'
                    else:
                        topic = 'ecommerce-events'
                    
                    self.producer.send(topic, value=event)
                
                # Flush and wait
                self.producer.flush()
                time.sleep(1)
                
                print(f"Generated {events_per_second} events at {datetime.now()}")
                
            except Exception as e:
                print(f"Error generating data: {e}")
                time.sleep(5)

if __name__ == "__main__":
    generator = EcommerceDataGenerator()
    generator.start_streaming(events_per_second=50)