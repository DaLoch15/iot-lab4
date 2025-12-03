# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np


# ============================================================================
# CONFIGURATION - Update these paths to match your setup
# ============================================================================

# Vehicle IDs (matches your file names: vehicle51.csv to vehicle55.csv)
device_st = 51
device_end = 56  # 51, 52, 53, 54, 55

# Path to the dataset
data_path = "data/vehicle{}.csv"

# Path to your certificates
certificate_formatter = "certificates/certs/Vehicle{:03d}-cert.pem"
key_formatter = "certificates/certs/Vehicle{:03d}-private.key"

# AWS IoT Endpoint
IOT_ENDPOINT = "a2726xers4vyvl-ats.iot.us-east-2.amazonaws.com"

# ============================================================================


class MQTTClient:
    def __init__(self, device_id, cert, key):
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(f"Vehicle{device_id}")
        
        # Configure endpoint
        self.client.configureEndpoint(IOT_ENDPOINT, 8883)
        self.client.configureCredentials("./keys/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)
        self.client.configureDrainingFrequency(2)
        self.client.configureConnectDisconnectTimeout(10)
        self.client.configureMQTTOperationTimeout(5)
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self, message):
        """
        Callback when a message is received.
        Receives processed results from the Greengrass component.
        """
        print("\n" + "="*60)
        print("📥 RECEIVED RESULT FROM GREENGRASS:")
        print(f"   Device: Vehicle{self.device_id}")
        print(f"   Topic: {message.topic}")
        try:
            payload = json.loads(message.payload.decode('utf-8'))
            print(f"   Vehicle ID: {payload.get('vehicle_id', 'N/A')}")
            print(f"   Max CO2: {payload.get('max_CO2', 'N/A')}")
            print(f"   Readings Processed: {payload.get('readings_processed', 'N/A')}")
        except:
            print(f"   Payload: {message.payload.decode('utf-8')}")
        print("="*60)


    def customSubackCallback(self, mid, data):
        print(f"   ✓ Subscription confirmed for Vehicle{self.device_id}")


    def customPubackCallback(self, mid):
        pass


    def publish_emission(self, max_rows=10):
        """
        Publish vehicle emission data row by row.
        
        Publishes to: vehicle/{vehicle_id}/emission
        Payload: {"vehicle_id": "51", "vehicle_CO2": 2416.04}
        """
        topic = f"vehicle/{self.device_id}/emission"
        
        try:
            df = pd.read_csv(data_path.format(self.device_id))
            rows_to_send = min(len(df), max_rows)
            
            print(f"\n📤 Vehicle{self.device_id} publishing {rows_to_send} readings to {topic}")
            
            for index, row in df.head(rows_to_send).iterrows():
                # Create payload with vehicle_id and vehicle_CO2
                payload_dict = {
                    "vehicle_id": self.device_id,
                    "vehicle_CO2": float(row['vehicle_CO2'])
                }
                
                payload = json.dumps(payload_dict)
                
                print(f"   [{index+1}/{rows_to_send}] CO2: {payload_dict['vehicle_CO2']}")
                self.client.publishAsync(topic, payload, 1, ackCallback=self.customPubackCallback)
                
                time.sleep(0.3)
                
            print(f"   ✓ Vehicle{self.device_id} finished publishing")
            
        except FileNotFoundError:
            print(f"   ✗ Data file not found: {data_path.format(self.device_id)}")
        except Exception as e:
            print(f"   ✗ Error: {str(e)}")


    def publish_batch(self, max_rows=20):
        """
        Publish all emission data as a single batch message.
        The component will calculate max across all readings.
        """
        topic = f"vehicle/{self.device_id}/emission"
        
        try:
            df = pd.read_csv(data_path.format(self.device_id))
            rows_to_send = min(len(df), max_rows)
            
            print(f"\n📤 Vehicle{self.device_id} publishing batch of {rows_to_send} readings")
            
            # Create array of records
            records = []
            for index, row in df.head(rows_to_send).iterrows():
                record = {
                    "vehicle_id": self.device_id,
                    "vehicle_CO2": float(row['vehicle_CO2'])
                }
                records.append(record)
            
            payload = json.dumps(records)
            print(f"   Publishing to: {topic}")
            self.client.publishAsync(topic, payload, 1, ackCallback=self.customPubackCallback)
            print(f"   ✓ Batch published")
            
        except FileNotFoundError:
            print(f"   ✗ Data file not found: {data_path.format(self.device_id)}")
        except Exception as e:
            print(f"   ✗ Error: {str(e)}")


    def subscribe_to_results(self):
        """
        Subscribe to receive processed results from Greengrass.
        
        Per Lab PDF Section 2.2: Each vehicle gets its own result topic
        so it only receives its own data.
        
        Subscribes to: iot/Vehicle_{vehicle_id}/result
        """
        result_topic = f"iot/Vehicle_{self.device_id}/result"
        
        print(f"   Vehicle{self.device_id} subscribing to: {result_topic}")
        self.client.subscribeAsync(result_topic, 1, ackCallback=self.customSubackCallback)


# ============================================================================
# Main Program
# ============================================================================

print("="*60)
print("  Lab 4 - Vehicle Emission Data Simulator")
print("  Sends CO2 data to Greengrass for processing")
print("="*60)

# Check for vehicle data files
print("\n📁 Checking for vehicle data files...")
available_vehicles = []
for i in range(device_st, device_end):
    try:
        df = pd.read_csv(data_path.format(i))
        available_vehicles.append(i)
        print(f"   ✓ Found vehicle{i}.csv ({len(df)} rows)")
    except FileNotFoundError:
        print(f"   ✗ Not found: vehicle{i}.csv")

if not available_vehicles:
    print("\n❌ No vehicle data files found!")
    print(f"   Expected files: vehicle51.csv to vehicle55.csv in data/ folder")
    exit(1)

print(f"\n   Found {len(available_vehicles)} vehicle data files")

# Connect MQTT clients
print("\n🔌 Connecting to AWS IoT...")
print(f"   Endpoint: {IOT_ENDPOINT}")

clients = []
for device_id in available_vehicles:
    try:
        cert_path = certificate_formatter.format(device_id)
        key_path = key_formatter.format(device_id)
        
        print(f"\n   Connecting Vehicle{device_id:03d}...")
        print(f"      Cert: {cert_path}")
        print(f"      Key:  {key_path}")
        
        client = MQTTClient(device_id, cert_path, key_path)
        client.client.connect()
        print(f"      ✓ Connected!")
        
        clients.append(client)
    except Exception as e:
        print(f"      ✗ Failed: {str(e)}")

if not clients:
    print("\n❌ No devices connected!")
    print("   Check your certificate paths and ensure they exist.")
    exit(1)

print(f"\n✅ {len(clients)} vehicles connected successfully!")

print("\n" + "="*60)
print("COMMANDS:")
print("  r - Subscribe to RESULTS (do this first!)")
print("  s - SEND emission data (10 readings per vehicle)")
print("  b - Send BATCH data (20 readings at once)")
print("  d - DISCONNECT and exit")
print("="*60)

while True:
    print("\nEnter command (r/s/b/d):")
    x = input().strip().lower()
    
    if x == "r":
        print("\n📡 Subscribing to result topics...")
        for c in clients:
            c.subscribe_to_results()
        time.sleep(1)
        print("\n✓ All vehicles subscribed!")
        print("  Now send data with 's' or 'b' to see results")
    
    elif x == "s":
        print("\n📤 Sending emission data (row by row)...")
        for c in clients:
            c.publish_emission(max_rows=10)
            time.sleep(0.5)
        print("\n✓ Done! Watch for results above ☝️")
    
    elif x == "b":
        print("\n📤 Sending BATCH emission data...")
        for c in clients:
            c.publish_batch(max_rows=20)
            time.sleep(0.5)
        print("\n✓ Done! Watch for results above ☝️")
    
    elif x == "d":
        print("\n🔌 Disconnecting...")
        for c in clients:
            c.client.disconnect()
        print("✓ Disconnected. Goodbye!")
        exit()
    
    else:
        print("Invalid command. Use: r, s, b, or d")

    time.sleep(0.5)
