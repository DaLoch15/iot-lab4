import sys
import json
import time
import logging
import traceback
import concurrent.futures

# Greengrass V2 IPC
import awsiot.greengrasscoreipc
import awsiot.greengrasscoreipc.client as client
from awsiot.greengrasscoreipc.model import (
    SubscribeToIoTCoreRequest,
    PublishToIoTCoreRequest,
    QOS,
    IoTCoreMessage
)

# Import our emission processor
from process_emission import VehicleEmissionProcessor

# Configure logging
logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    stream=sys.stdout,
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class StreamHandler(client.SubscribeToIoTCoreStreamHandler):
    """Handler for incoming MQTT messages from IoT Core"""
    
    def __init__(self, processor):
        super().__init__()
        self.processor = processor
    
    def on_stream_event(self, event: IoTCoreMessage) -> None:
        """Called when a message is received"""
        try:
            topic = event.message.topic_name
            payload = event.message.payload.decode('utf-8')
            logger.info(f"Received message on {topic}")
            self.processor.handle_message(topic, payload)
        except Exception as e:
            logger.error(f"Error in stream handler: {e}")
            traceback.print_exc()
    
    def on_stream_error(self, error: Exception) -> bool:
        logger.error(f"Stream error: {error}")
        return True  # Return True to close stream, False to keep it open
    
    def on_stream_closed(self) -> None:
        logger.info("Stream closed")


class VehicleEmissionComponent:
    """Main Greengrass Component for Vehicle Emission Processing"""
    
    def __init__(self, config):
        self.config = config
        self.ipc_client = None
        self.processor = VehicleEmissionProcessor()
        self.subscriptions = []
        
    def initialize(self):
        """Initialize IPC connection and subscriptions"""
        try:
            # Connect to Greengrass IPC
            self.ipc_client = awsiot.greengrasscoreipc.connect()
            logger.info("Connected to Greengrass IPC")
            
            # Share IPC client with processor
            self.processor.ipc_client = self.ipc_client
            
            # Subscribe to vehicle emission topics
            # Per Lab PDF: Subscribe to topics where vehicle data is published
            subscribe_topics = self.config.get('mqtt-subscribe-topics', [])
            
            for topic in subscribe_topics:
                self._subscribe_to_topic(topic)
                
            logger.info("Component initialization complete")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            traceback.print_exc()
            return False
    
    def _subscribe_to_topic(self, topic):
        """Subscribe to an MQTT topic via IoT Core"""
        try:
            request = SubscribeToIoTCoreRequest()
            request.topic_name = topic
            request.qos = QOS.AT_LEAST_ONCE
            
            handler = StreamHandler(self.processor)
            operation = self.ipc_client.new_subscribe_to_iot_core(handler)
            operation.activate(request)
            
            # Wait for subscription to be established
            future = operation.get_response()
            future.result(timeout=10)
            
            self.subscriptions.append(operation)
            logger.info(f"Subscribed to topic: {topic}")
            
        except Exception as e:
            logger.error(f"Failed to subscribe to {topic}: {e}")
            traceback.print_exc()
    
    def run(self):
        """Main run loop - keeps component alive"""
        logger.info("Vehicle Emission Component running...")
        
        try:
            while True:
                # Component stays alive to process incoming messages
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Component shutdown requested")
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            traceback.print_exc()
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up component...")
        for subscription in self.subscriptions:
            try:
                subscription.close()
            except:
                pass


def main():
    """Entry point"""
    try:
        # Parse configuration from command line
        # The recipe passes config as JSON string
        if len(sys.argv) > 1:
            config = json.loads(sys.argv[1])
        else:
            # Default configuration for testing
            config = {
                'base-pubsub-topic': 'vehicle-emission',
                'mqtt-subscribe-topics': [
                    'vehicle/+/emission',     # Wildcard for all vehicles
                    'iot/emission/#'          # Alternative topic pattern
                ]
            }
        
        logger.info(f"Component config: {json.dumps(config, indent=2)}")
        
        # Create and run component
        component = VehicleEmissionComponent(config)
        
        if component.initialize():
            component.run()
        else:
            logger.error("Failed to initialize component")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
