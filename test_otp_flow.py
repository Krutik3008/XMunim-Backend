import requests
import json

BASE_URL = "http://localhost:8000"

def test_otp_flow():
    phone = "1234567890"
    
    # 1. Send OTP
    print(f"\n--- Testing Send OTP for {phone} ---")
    payload = {
        "phone": phone,
        "name": "Test User",
        "is_login": False
    }
    try:
        response = requests.post(f"{BASE_URL}/api/auth/send-otp", json=payload)
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        if response.status_code != 200:
            print("Failed to send OTP. Check if server is running on port 8000.")
            return
    except Exception as e:
        print(f"Error connecting to server: {e}")
        return

    # 2. Verify OTP (Mock 123456)
    print(f"\n--- Testing Verify OTP for {phone} with 123456 ---")
    payload = {
        "phone": phone,
        "otp": "123456",
        "name": "Test User",
        "terms_accepted": True
    }
    response = requests.post(f"{BASE_URL}/api/auth/verify-otp", json=payload)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json().get('message')}")
    
    if response.status_code == 200:
        print("SUCCESS: Mock OTP flow verified!")
    else:
        print("FAILED: Mock OTP flow failed!")

if __name__ == "__main__":
    test_otp_flow()
