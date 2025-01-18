import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

// Load CSV Data
const csvData = new SharedArray('patient_data', function () {
    return open('./synthetic_patient_data_final.csv')
        .split('\n') // Split into lines
        .slice(1)    // Remove header
        .map(line => {
            const fields = line.split(',');
            return {
                device_id: fields[0],
                measurement_type: fields[1],
                timestamp: fields[2],
                raw_value: fields[3],
                battery: Number(fields[4]),
                signal_strength: Number(fields[5])
            };
        });
});

// K6 Configuration
export const options = {
    vus: 100,           // Virtual Users
    duration: '1m',     // Test duration
    thresholds: {
        http_req_duration: ['p(95)<500'], // 95% < 500ms
        http_req_failed: ['rate<0.01']   // Failures < 1%
    },
};

// Kafka REST Proxy Endpoint
const kafka_url = 'http://localhost:8082/topics/measurements'; // Replace with your REST Proxy URL

// Kafka Headers
const params = {
    headers: {
        'Content-Type': 'application/vnd.kafka.json.v2+json', // Kafka REST Proxy JSON format
    },
};

// K6 Execution
export default function () {
    // Pick a random data point
    const data = csvData[Math.floor(Math.random() * csvData.length)];

    // Simulate delay if specified
    if (data.delay) {
        sleep(Math.random() * 3); // Delay up to 3 seconds
    }

    // Kafka Message Payload
    const payload = JSON.stringify({
        records: [
            {
                value: {
                    device_id: data.device_id,
                    measurement_type: data.measurement_type,
                    timestamp: data.timestamp,
                    raw_value: data.raw_value,
                    battery: data.battery,
                    signal_strength: data.signal_strength
                }
            }
        ]
    });

    // Send data to Kafka
    const res = http.post(kafka_url, payload, params);

    // Check response
    check(res, {
        'is status 200': (r) => r.status === 200,
        'response time < 500ms': (r) => r.timings.duration < 500,
    });

    // Simulate slight delay between requests
    sleep(0.1);
}
