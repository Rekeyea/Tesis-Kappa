FLINK_API="http://localhost:10210/v3"

SESSION_RESPONSE=$(curl -s -X POST "${FLINK_API}/sessions" -H "Content-Type: application/json" -d '{
    "sessionName": "shared-session",
    "planner": "blink"
}') 

SESSION_ID=$(echo "$SESSION_RESPONSE" | jq -r '.sessionHandle')

echo ${SESSION_ID}