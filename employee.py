import socket
import json
import time
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Sample organizations
organizations = [
    "Acme Corp",
    "Globex Inc",
    "Initech",
    "Umbrella Corp",
    "Wayne Enterprises",
    "Stark Industries",
    "Hooli",
    "Pied Piper"
]

def generate_employee_data(num_employees=10):
    """
    Generate random employee data from different organizations.
    """
    employees = []
    
    for _ in range(num_employees):
        employee = {
            'name': fake.name(),
            'email': fake.email(),
            'job_title': fake.job(),
            'organization': random.choice(organizations),
            'address': fake.address(),
            'phone_number': fake.phone_number(),
            'date_of_birth': fake.date_of_birth(minimum_age=22, maximum_age=65).isoformat(),
            'timestamp': time.time()  # Current timestamp in seconds since epoch
        }
        employees.append(employee)
    
    return employees

def send_data_to_udp(data, address='127.0.0.1', port=2056):
    """
    Send JSON data to a UDP port.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for item in data:
        json_data = json.dumps(item)  # Convert to JSON string
        sock.sendto(json_data.encode('utf-8'), (address, port))
        print(f"Sent data to {address}:{port} - {json_data}")
    sock.close()

def main():
    num_employees = 10  # Number of employees to generate
    employees = generate_employee_data(num_employees)
    send_data_to_udp(employees)

if __name__ == "__main__":
    main()

