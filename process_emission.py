import json
import logging
import sys
import traceback

# Greengrass V2 IPC imports
import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    PublishToIoTCoreRequest,
    QOS,
    SubscribeToIoTCoreRequest
)

# Configure logging
logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    stream=sys.stdout,
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class VehicleEmissionProcessor:
    def __init__(self):
        self.ipc_client = None
        self.vehicle_data = {}  # Store data per vehicle to calculate max
        
    def connect_ipc(self):
        # connect ipc 
        try:
            self.ipc_client = awsiot.greengrasscoreipc.connect()
            logger.info("Successfully connected to Greengrass IPC")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to IPC: {e}")
            return False
    
    def process_emission_data(self, vehicle_id, vehicle_co2):
       # get max C02
        try:
            co2_val = float(vehicle_co2)
            
            # track max CO2 per vehicle
            if vehicle_id not in self.vehicle_data:
                self.vehicle_data[vehicle_id] = {
                    'max_co2': co2_val,
                    'readings_count': 1
                }
            else:
                if co2_val > self.vehicle_data[vehicle_id]['max_co2']:
                    self.vehicle_data[vehicle_id]['max_co2'] = co2_val
                self.vehicle_data[vehicle_id]['readings_count'] += 1
            
            result = {
                'vehicle_id': vehicle_id,
                'max_CO2': self.vehicle_data[vehicle_id]['max_co2'],
                'readings_processed': self.vehicle_data[vehicle_id]['readings_count']
            }
            
            logger.info(f"Processed emission for {vehicle_id}: max_CO2={result['max_CO2']}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing emission data: {e}")
            return None
    
    def publish_result(self, vehicle_id, result):
       #publish back the max co2
        if not self.ipc_client:
            logger.error("IPC client not connected")
            return False
            
        try:
            # Use vehicle-specific topic to prevent cross-device data leakage
            topic = f"iot/Vehicle_{vehicle_id}/result"
            payload = json.dumps(result)
            
            request = PublishToIoTCoreRequest()
            request.topic_name = topic
            request.qos = QOS.AT_LEAST_ONCE
            request.payload = payload.encode('utf-8')
            
            operation = self.ipc_client.new_publish_to_iot_core()
            operation.activate(request)
            future = operation.get_response()
            future.result(timeout=10)
            
            logger.info(f"Published result to topic: {topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish result: {e}")
            traceback.print_exc()
            return False
    
    def handle_message(self, topic, payload):
        
        try:
            data = json.loads(payload)
            
            # Handle both single record and array of records
            if isinstance(data, list):
                # Array of records - process each and find max
                max_result = None
                for record in data:
                    vehicle_id = record.get('vehicle_id')
                    vehicle_co2 = record.get('vehicle_CO2')
                    if vehicle_id and vehicle_co2:
                        result = self.process_emission_data(vehicle_id, vehicle_co2)
                        if result:
                            max_result = result
                
                # Publish final result after processing all records
                if max_result:
                    self.publish_result(max_result['vehicle_id'], max_result)
            else:
                # Single record
                vehicle_id = data.get('vehicle_id')
                vehicle_co2 = data.get('vehicle_CO2')
                
                if vehicle_id and vehicle_co2:
                    result = self.process_emission_data(vehicle_id, vehicle_co2)
                    if result:
                        self.publish_result(vehicle_id, result)
                else:
                    logger.warning(f"Invalid message format: {data}")
                    
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            traceback.print_exc()


# Global processor instance
processor = VehicleEmissionProcessor()


def on_message_received(topic, payload):
    """Callback for received messages"""
    logger.info(f"Received message on topic: {topic}")
    processor.handle_message(topic, payload)
