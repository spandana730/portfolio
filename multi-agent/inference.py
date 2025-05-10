import os
import requests

# Replace this with your actual endpoint URL
API_URL = "https://lgj704z9p0j2vf79.us-east4.gcp.endpoints.huggingface.cloud"  # e.g., https://mistral-rw123.hf.space
HF_ENDPOINT_TOKEN = os.environ.get("HF_ENDPOINT_TOKEN")

headers = {
    "Authorization": f"Bearer {HF_ENDPOINT_TOKEN}",
    "Content-Type": "application/json"
}

def call_model(prompt: str) -> str:
    response = requests.post(
        f"{API_URL}/generate",  # <-- use /generate for HF endpoints
        headers=headers,
        json={
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 2048,
                "temperature": 0.3,
                "do_sample": False
            }
        }
    )

    if response.status_code != 200:
        raise RuntimeError(f"Inference error: {response.status_code} - {response.text}")

    result = response.json()
    
    # Handle variations in response format
    if isinstance(result, dict) and "generated_text" in result:
        return result["generated_text"]
    elif isinstance(result, list) and "generated_text" in result[0]:
        return result[0]["generated_text"]
    elif "text" in result:
        return result["text"]
    else:
        return "⚠️ No output generated."
