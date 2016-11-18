create_job_schema = {
    "type": "object",
    "properties": {
        "task": {"type": "string", "minLength": 5},
        "arguments": {"type": "object", "minProperties": 1},
    },
    "required": ["task", "arguments"]
}
