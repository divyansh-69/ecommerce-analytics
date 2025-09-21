import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

class EcommerceStreamProcessor:
    def __init__(self):
        # Initialize Kafka Consumer
        self.consumer = KafkaConsumer(
            'ecommerce-events',
            'ecommerce-transactions',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='ecommerce-processor',
            auto_offset_reset='latest'
        )
        
        # Initialize Elasticsearch
        self.es = Elasticsearch(['http://localhost:9200'])
        
        # Batch processing variables
        self.batch_size = 10
        self.batch_events = []
        self.total_processed = 0
        
    def connect_services(self):
        """Test connections to Kafka and Elasticsearch"""
        try:
            # Test Kafka connection
            self.consumer.poll(timeout_ms=1000)
            print("Connected to Kafka successfully!")
            
            # Test Elasticsearch connection
            if self.es.ping():
                print("Connected to Elasticsearch successfully!")
                
                # Create index template if it doesn't exist
                self.create_index_template()
            else:
                raise Exception("Cannot connect to Elasticsearch")
                
        except Exception as e:
            print(f"Connection error: {e}")
            return False
        
        return True
    
    def create_index_template(self):
        """Create index template for ecommerce events"""
        template = {
            "index_patterns": ["ecommerce-events*"],
            "template": {
                "mappings": {
                    "properties": {
                        "event_id": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "customer_id": {"type": "keyword"},
                        "session_id": {"type": "keyword"},
                        "event_type": {"type": "keyword"},
                        "product_id": {"type": "keyword"},
                        "product_name": {"type": "text"},
                        "category": {"type": "keyword"},
                        "price": {"type": "float"},
                        "quantity": {"type": "integer"},
                        "processing_time": {"type": "date"},
                        "location": {
                            "properties": {
                                "city": {"type": "keyword"},
                                "country": {"type": "keyword"},
                                "latitude": {"type": "float"},
                                "longitude": {"type": "float"}
                            }
                        }
                    }
                }
            }
        }
        
        try:
            self.es.indices.put_index_template(name="ecommerce-template", body=template)
            print("Index template created successfully!")
        except Exception as e:
            print(f"Template creation warning: {e}")
    
    def process_event(self, event):
        """Process and enrich a single event"""
        # Add processing timestamp
        event['processing_time'] = datetime.now().isoformat()
        
        # Add derived fields
        if 'timestamp' in event:
            try:
                timestamp = datetime.fromisoformat(event['timestamp'])
                event['hour'] = timestamp.hour
                event['day_of_week'] = timestamp.weekday()
            except:
                pass
        
        return event
    
    def send_batch_to_elasticsearch(self):
        """Send batch of events to Elasticsearch"""
        if not self.batch_events:
            return
        
        try:
            # Prepare bulk operations
            bulk_operations = []
            for event in self.batch_events:
                # Index operation
                bulk_operations.append({
                    "index": {
                        "_index": "ecommerce-events",
                        "_id": event.get('event_id', None)
                    }
                })
                bulk_operations.append(event)
            
            # Send bulk request
            response = self.es.bulk(body=bulk_operations)
            
            # Check for errors
            if response.get('errors', False):
                print(f"Some events failed to index: {response}")
            
            self.total_processed += len(self.batch_events)
            print(f"Processed batch of {len(self.batch_events)} events | Total: {self.total_processed}")
            
            # Clear batch
            self.batch_events = []
            
        except Exception as e:
            print(f"Error sending batch to Elasticsearch: {e}")
            # Keep events in batch for retry
    
    def start_processing(self):
        """Main processing loop"""
        print("Starting real-time event processing...")
        print("Press Ctrl+C to stop")
        
        try:
            for message in self.consumer:
                # Process the event
                event = self.process_event(message.value)
                
                # Print event summary
                event_type = event.get('event_type', 'unknown')
                product_name = event.get('product_name', 'unknown')
                category = event.get('category', 'unknown')
                print(f"Received: {event_type} | {product_name} | {category}")
                
                # Add to batch
                self.batch_events.append(event)
                
                # Send batch when it reaches batch_size
                if len(self.batch_events) >= self.batch_size:
                    self.send_batch_to_elasticsearch()
                
        except KeyboardInterrupt:
            print("\nShutting down processor...")
            # Send remaining events
            if self.batch_events:
                self.send_batch_to_elasticsearch()
            print(f"Final total processed: {self.total_processed}")
        
        except Exception as e:
            print(f"Processing error: {e}")
        
        finally:
            self.consumer.close()

def main():
    processor = EcommerceStreamProcessor()
    
    # Test connections
    if processor.connect_services():
        # Start processing
        processor.start_processing()
    else:
        print("Failed to connect to services. Please check Kafka and Elasticsearch.")

if __name__ == "__main__":
    main()