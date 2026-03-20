from fastapi import FastAPI, APIRouter, HTTPException, Depends, Body
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
import pytz
from datetime import datetime, timezone, timedelta
import jwt
import random
import io
import base64
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
import string
import firebase_admin
from firebase_admin import credentials, messaging
import asyncio
import requests
from bson import ObjectId

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("server")

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ.get('DB_NAME', 'xmunim_app')]

# Create the main app without a prefix
app = FastAPI(title="XMunim App Backend", version="1.0.0")

# Mount static files for public assets
if not (ROOT_DIR / "static").exists():
    (ROOT_DIR / "static").mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(ROOT_DIR / "static")), name="static")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# JWT Configuration
JWT_SECRET = "xmunim_secret_key_2024"
security = HTTPBearer()

# Firebase Admin Configuration
try:
    if not firebase_admin._apps:
        # 1. Try to load from environment variable (Best for Render/Production)
        firebase_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON')
        if firebase_json:
            import json
            firebase_info = json.loads(firebase_json)
            cred = credentials.Certificate(firebase_info)
            firebase_admin.initialize_app(cred)
            logger.info("Firebase Admin initialized successfully via environment variable.")
        else:
            # 2. Fallback to local file (Best for Local Development)
            firebase_creds_path = ROOT_DIR / 'firebase-service-account.json'
            if firebase_creds_path.exists():
                cred = credentials.Certificate(str(firebase_creds_path))
                firebase_admin.initialize_app(cred)
                logger.info("Firebase Admin initialized successfully via local file.")
            else:
                logger.error("CRITICAL: Firebase credentials not found!")
    else:
        logger.info("Firebase Admin already initialized.")
except Exception as e:
    logger.error(f"Failed to initialize Firebase Admin: {e}")

# ==================== Pydantic Models ====================

class User(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    phone: str
    name: str
    active_role: str = "customer"  # "customer", "shop_owner", or "admin"
    admin_roles: List[str] = []  # List of admin roles: ["admin", "super_admin"]
    verified: bool = False
    flagged: bool = False
    terms_accepted: bool = False  # Terms of Services & Privacy Policy acceptance
    profile_photo: Optional[str] = None  # Base64-encoded profile photo
    fcm_token: Optional[str] = None  # Firebase Cloud Messaging token for push notifications
    push_enabled: bool = True
    payment_alerts_enabled: bool = True
    promotions_enabled: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Shop(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    owner_id: str
    name: str
    category: str
    pincode: Optional[str] = None
    city: Optional[str] = None
    area: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    location: Optional[str] = None
    gst_number: Optional[str] = None
    upi_id: Optional[str] = None
    shop_code: str = Field(default_factory=lambda: ''.join(random.choices(string.ascii_uppercase + string.digits, k=8)))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Customer(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str
    name: str
    phone: str
    nickname: Optional[str] = None
    type: str = "customer" # "customer", "staff", or "services"
    balance: float = 0.0  # negative means customer owes money
    is_auto_reminder_enabled: bool = False
    auto_reminder_delay: str = "3 days overdue"
    auto_reminder_frequency: str = "Daily until paid"
    auto_reminder_method: str = "Push Notification"
    auto_reminder_message: Optional[str] = None
    is_verified: bool = False
    
    # Staff/Services specific fields
    service_rate: Optional[float] = None
    service_rate_type: Optional[str] = None # 'daily', 'hourly', 'monthly'
    service_log: Optional[Dict[str, Any]] = None # {"2026-03-12": {"status": "present", "rate": 55}}
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Service(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str
    name: str
    phone: str
    nickname: Optional[str] = None
    category: Optional[str] = None # e.g. "Milk Delivery", "Cleaner"
    service_rate: float = 0.0
    service_rate_type: str = "daily" # 'daily', 'hourly', 'monthly'
    service_log: Optional[Dict[str, Any]] = None # {"2026-03-12": {"status": "present", "rate": 55}}
    balance: float = 0.0
    is_verified: bool = False
    type: str = "services"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Product(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str
    name: str
    price: float
    active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Staff(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str
    name: str
    phone: str
    nickname: Optional[str] = None
    role: Optional[str] = None # e.g. "Chef", "Delivery Boy"
    service_rate: float = 0.0
    service_rate_type: str = "daily" # 'daily', 'hourly', 'monthly'
    service_log: Optional[Dict[str, Any]] = None 
    balance: float = 0.0
    upi_id: Optional[str] = None
    is_verified: bool = False
    type: str = "staff"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class TransactionProduct(BaseModel):
    product_id: str
    name: str
    price: float
    quantity: int = 1
    subtotal: float

class Transaction(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str
    customer_id: str
    type: str  # "credit" (udhaar) or "debit" (jama)
    amount: float
    products: Optional[List[TransactionProduct]] = None
    note: Optional[str] = None
    date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PaymentRequest(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str
    customer_id: str
    amount: float
    method: str  # "Push Notification", "SMS", "WhatsApp"
    title: str
    message: str
    status: str = "sent"  # "sent", "delivered", "failed", "pending"
    scheduled_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class UserSession(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    phone: Optional[str] = None
    device: str
    os: str
    last_active: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ==================== Request Models ====================

class AuthRequest(BaseModel):
    phone: str
    name: Optional[str] = None
    is_login: bool = False
    terms_accepted: bool = False

class OTPVerifyRequest(BaseModel):
    phone: str
    otp: str
    name: Optional[str] = None
    terms_accepted: bool = False

class RoleSwitchRequest(BaseModel):
    role: str

class AssignRoleRequest(BaseModel):
    user_id: str
    admin_roles: List[str]
    action: str  # "grant" or "revoke"

class JoinShopRequest(BaseModel):
    shop_code: str
    customer_name: Optional[str] = None

class ShopCreateRequest(BaseModel):
    name: str
    category: str
    pincode: str
    city: str
    area: str
    state: str
    country: str
    location: Optional[str] = None
    gst_number: Optional[str] = None
    upi_id: Optional[str] = None

class ShopUpdateRequest(BaseModel):
    name: Optional[str] = None
    category: Optional[str] = None
    pincode: Optional[str] = None
    city: Optional[str] = None
    area: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    location: Optional[str] = None
    gst_number: Optional[str] = None
    upi_id: Optional[str] = None

class CustomerCreateRequest(BaseModel):
    name: str
    phone: str
    nickname: Optional[str] = None
    type: Optional[str] = "customer"
    is_auto_reminder_enabled: Optional[bool] = False
    auto_reminder_delay: Optional[str] = "3 days overdue"
    auto_reminder_frequency: Optional[str] = "Daily until paid"
    auto_reminder_method: Optional[str] = "Push Notification"
    auto_reminder_message: Optional[str] = None
    
    # Staff/Services specific fields
    service_rate: Optional[float] = None
    service_rate_type: Optional[str] = None
    service_log: Optional[Dict[str, Any]] = None

class ServiceCreateRequest(BaseModel):
    name: str
    phone: str
    nickname: Optional[str] = None
    category: Optional[str] = None
    service_rate: float
    service_rate_type: str = "daily"

class ProductCreateRequest(BaseModel):
    name: str
    price: float

class ProductUpdateRequest(BaseModel):
    name: Optional[str] = None
    price: Optional[float] = None
    active: Optional[bool] = None

class StaffCreateRequest(BaseModel):
    name: str
    phone: str
    nickname: Optional[str] = None
    role: Optional[str] = None
    service_rate: float
    service_rate_type: str = "daily"
    upi_id: Optional[str] = None

class StaffUpdateRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    nickname: Optional[str] = None
    role: Optional[str] = None
    service_rate: Optional[float] = None
    service_rate_type: Optional[str] = None
    service_log: Optional[Dict[str, Any]] = None
    upi_id: Optional[str] = None

class TransactionProductRequest(BaseModel):
    product_id: str
    quantity: int = 1

class TransactionCreateRequest(BaseModel):
    customer_id: str
    type: str
    amount: Optional[float] = None
    products: List[TransactionProductRequest] = []
    note: str = ""

class CustomerUpdateRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    nickname: Optional[str] = None
    type: Optional[str] = None
    is_auto_reminder_enabled: Optional[bool] = None
    auto_reminder_delay: Optional[str] = None
    auto_reminder_frequency: Optional[str] = None
    auto_reminder_method: Optional[str] = None
    
    # Staff/Services specific fields
    service_rate: Optional[float] = None
    service_rate_type: Optional[str] = None
    service_log: Optional[Dict[str, Any]] = None
    auto_reminder_message: Optional[str] = None

class ServiceUpdateRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    nickname: Optional[str] = None
    category: Optional[str] = None
    service_rate: Optional[float] = None
    service_rate_type: Optional[str] = None
    service_log: Optional[Dict[str, Any]] = None

class UserVerifyRequest(BaseModel):
    verified: Optional[bool] = None
    flagged: Optional[bool] = None

class UserUpdateRequest(BaseModel):
    name: Optional[str] = None
    fcm_token: Optional[str] = None
    push_enabled: Optional[bool] = None
    payment_alerts_enabled: Optional[bool] = None
    promotions_enabled: Optional[bool] = None

class ProfilePhotoRequest(BaseModel):
    photo: str  # Base64-encoded image string

class PushNotificationRequest(BaseModel):
    title: str
    body: str
    data: Optional[dict] = None
    method: Optional[str] = "Push Notification"
    scheduled_at: Optional[datetime] = None

# ==================== Helper Functions ====================

def create_token(user_id: str, session_id: str) -> str:
    now = datetime.now(timezone.utc)
    expiry = now.timestamp() + (100 * 365 * 24 * 60 * 60)  # 100 years (Lifetime)
    payload = {
        "user_id": user_id,
        "session_id": session_id,
        "exp": int(expiry),
        "iat": int(now.timestamp())
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

async def verify_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload.get("user_id")
        session_id = payload.get("session_id")
        
        if not user_id or not session_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")
            
        # Check if session exists in DB
        session = await db.sessions.find_one({"id": session_id, "user_id": user_id})
        if not session:
            raise HTTPException(status_code=401, detail="Session expired or logged out")
            
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        print(f"JWT Error: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")
    except HTTPException:
        raise
    except Exception as e:
        print(f"JWT Verification Error: {e}")
        raise HTTPException(status_code=401, detail="Token verification failed")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    payload = await verify_token(credentials.credentials)
    user_id = payload["user_id"]
    user = await db.users.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Update last active time for the session
    await db.sessions.update_one(
        {"id": payload["session_id"]},
        {"$set": {"last_active": datetime.now(timezone.utc)}}
    )
    
    return User(**parse_from_mongo(user))

async def get_admin_user(current_user: User = Depends(get_current_user)):
    has_admin_access = (
        current_user.active_role == "admin" and
        ("admin" in current_user.admin_roles or "super_admin" in current_user.admin_roles)
    )
    
    if not has_admin_access:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user

def prepare_for_mongo(data):
    """Keep datetime objects as is for MongoDB BSON Date support, or convert if needed"""
    # MongoDB handles datetime objects natively as BSON Dates
    # We only convert if we specifically want strings
    return data

def parse_from_mongo(item):
    """Parse datetime strings or objects back from MongoDB and handle ID mapping"""
    if item is None:
        return None
    if isinstance(item, dict):
        item = item.copy()  # Don't modify original
        # Map MongoDB's internal _id to our standard 'id' field if 'id' is missing
        if '_id' in item:
            if 'id' not in item:
                item['id'] = str(item['_id'])
            item.pop('_id')
        
        # Parse common datetime fields
        for key, value in item.items():
            if isinstance(value, str) and key in ['created_at', 'date', 'scheduled_at']:
                try:
                    item[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except:
                    pass
            elif isinstance(value, datetime):
                # Ensure all datetimes are offset-aware for comparison (Motor usually returns naive UTC)
                if value.tzinfo is None:
                    item[key] = value.replace(tzinfo=timezone.utc)
    return item

# ==================== Authentication Routes ====================

@api_router.post("/auth/send-otp")
async def send_otp(request: AuthRequest):
    """Send OTP to phone number"""
    # Search for user by phone number (for logging or internal logic, but don't block)
    user_exists = await db.users.find_one({"phone": request.phone})
    
    # We no longer block sending OTP based on user existence. 
    # Any number can receive an OTP, and the verification step will handle login vs signup.
    if not request.is_login and not request.name and not user_exists:
        # If it's a new user flow (SignUp), we still prefer a name if possible
        # but we won't strictly block here as verification can handle it
        pass

    msg91_auth_key = os.environ.get("MSG91_AUTH_KEY")
    msg91_template_id = os.environ.get("MSG91_TEMPLATE_ID")
    
    # Check if MSG91 is fully configured and not pending
    use_msg91 = msg91_auth_key and msg91_template_id and msg91_template_id.upper() != "PENDING"
    
    if use_msg91:
        url = "https://control.msg91.com/api/v5/otp"
        # Extract dial code assuming +91 or raw 10 digits
        mobile = request.phone.replace("+", "").replace(" ", "")
        if len(mobile) == 10:
            mobile = f"91{mobile}"
        
        payload = {
            "template_id": msg91_template_id,
            "mobile": mobile,
            "authkey": msg91_auth_key
        }
        try:
            # Using JSON payload according to MSG91 docs for API v5
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info(f"MSG91 Send OTP response: {response.text}")
            
            response_data = response.json()
            if response_data.get("type") == "error":
                error_msg = response_data.get("message", "Failed to send OTP")
                logger.error(f"MSG91 send failed: {error_msg}")
                raise HTTPException(status_code=400, detail=error_msg)
            else:
                # MSG91 returns a request id in the response 'message' field sometimes
                session_id = response_data.get("message", "")
                return {"message": "OTP sent successfully", "session": session_id}
        except Exception as e:
            logger.error(f"Failed to send OTP via MSG91: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to send OTP via provider")
    else:
        raise HTTPException(status_code=500, detail="OTP Provider not configured properly")

@api_router.get("/auth/check-phone/{phone}")
async def check_phone(phone: str):
    """Check if a user exists with this phone number"""
    user = await db.users.find_one({"phone": phone})
    return {"exists": user is not None}

@api_router.post("/auth/verify-otp")
async def verify_otp(request: OTPVerifyRequest):
    """Verify OTP and login user"""
    msg91_auth_key = os.environ.get("MSG91_AUTH_KEY")
    msg91_template_id = os.environ.get("MSG91_TEMPLATE_ID")
    
    # Flag to skip checking local DB if verified via MSG91
    is_verified = False
    
    # Enforce MSG91 verification
    use_msg91_verify = (
        msg91_auth_key and 
        msg91_template_id and 
        msg91_template_id.upper() != "PENDING"
    )
    
    if use_msg91_verify:
        url = "https://control.msg91.com/api/v5/otp/verify"
        mobile = request.phone.replace("+", "").replace(" ", "")
        if len(mobile) == 10:
            mobile = f"91{mobile}"
        
        params = {
            "otp": request.otp,
            "mobile": mobile
        }
        headers = {
            "authkey": msg91_auth_key
        }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            logger.info(f"MSG91 Verify OTP response: {response.text}")
            
            # API v5 returns type 'success' or 'error'
            if response_data.get("type") == "success":
                is_verified = True
            else:
                error_msg = response_data.get("message", "Invalid OTP")
                logger.error(f"MSG91 verification failed: {error_msg}")
                raise HTTPException(status_code=400, detail=error_msg)
        except requests.exceptions.HTTPError as he:
            try:
                error_data = he.response.json()
                error_msg = error_data.get("message", "Invalid OTP")
            except:
                error_msg = "Invalid OTP"
            raise HTTPException(status_code=400, detail=error_msg)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to verify OTP via MSG91: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to verify OTP with provider")
    else:
        raise HTTPException(status_code=500, detail="OTP Provider not configured properly")
    
    if not is_verified:
        raise HTTPException(status_code=400, detail="Invalid OTP")
    
    # Search for user by phone number only
    user_data = await db.users.find_one({"phone": request.phone})
    
    if not user_data:
        # Create new user, mark as verified since they used OTP
        user = User(phone=request.phone, name=request.name or "User", verified=True, terms_accepted=request.terms_accepted)
        user_dict = prepare_for_mongo(user.dict())
        await db.users.insert_one(user_dict)
    else:
        user = User(**parse_from_mongo(user_data))
        # If existing user is not verified, mark them as verified now
        if not user.verified:
            user.verified = True
            await db.users.update_one({"id": user.id}, {"$set": {"verified": True}})
    
    # Create a new session record
    new_session = UserSession(
        user_id=user.id,
        phone=user.phone,
        device="Android App" if "android" in (request.name or "").lower() else "Mobile App",
        os="Android" if "android" in (request.name or "").lower() else "iOS/Android"
    )
    await db.sessions.insert_one(prepare_for_mongo(new_session.dict()))
    
    token = create_token(user.id, new_session.id)
    
    return {
        "token": token,
        "user": user.dict(),
        "message": "Login successful"
    }

@api_router.post("/auth/verify-sdk")
async def verify_sdk(request: AuthRequest):
    """Log in user after MSG91 React Native SDK verifies the OTP successfully"""
    # Search for user by phone number only
    user_data = await db.users.find_one({"phone": request.phone})
    
    if request.is_login:
        if not user_data:
            raise HTTPException(status_code=404, detail="User does not exist. Please sign up.")
        user = User(**parse_from_mongo(user_data))
        # If existing user is not verified, mark them as verified now
        if not user.verified:
            user.verified = True
            await db.users.update_one({"id": user.id}, {"$set": {"verified": True}})
    else:
        # Sign Up case
        if user_data:
            raise HTTPException(status_code=400, detail="Phone number already registered. Please login.")
        
        # Create new user, mark as verified since they used SDK OTP
        user = User(phone=request.phone, name=request.name or "User", verified=True, terms_accepted=request.terms_accepted)
        user_dict = prepare_for_mongo(user.dict())
        await db.users.insert_one(user_dict)
    
    # Create a new session record
    new_session = UserSession(
        user_id=user.id,
        phone=user.phone,
        device="Android App" if "android" in (request.name or "").lower() else "Mobile App",
        os="Android" if "android" in (request.name or "").lower() else "iOS/Android"
    )
    await db.sessions.insert_one(prepare_for_mongo(new_session.dict()))
    
    token = create_token(user.id, new_session.id)
    
    return {
        "token": token,
        "user": user.dict(),
        "message": "Login successful"
    }

@api_router.post("/auth/switch-role")
async def switch_role(request: RoleSwitchRequest, current_user: User = Depends(get_current_user)):
    """Switch user role between customer, shop_owner, and admin"""
    if request.role not in ["customer", "shop_owner", "admin"]:
        raise HTTPException(status_code=400, detail="Invalid role")
    
    if request.role == "admin":
        has_admin_access = "admin" in current_user.admin_roles or "super_admin" in current_user.admin_roles
        
        if not has_admin_access:
            raise HTTPException(status_code=403, detail="Admin access not authorized")
    
    await db.users.update_one(
        {"id": current_user.id},
        {"$set": {"active_role": request.role}}
    )
    
    updated_user = await db.users.find_one({"id": current_user.id})
    return {"user": User(**parse_from_mongo(updated_user)).dict(), "message": "Role switched successfully"}

@api_router.get("/auth/me")
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current user information"""
    return current_user

@api_router.put("/auth/me")
async def update_current_user(request: UserUpdateRequest, current_user: User = Depends(get_current_user)):
    """Update current user profile"""
    update_data = {}
    if request.name is not None:
        update_data["name"] = request.name
    if request.fcm_token is not None:
        update_data["fcm_token"] = request.fcm_token
    if request.push_enabled is not None:
        update_data["push_enabled"] = request.push_enabled
    if request.payment_alerts_enabled is not None:
        update_data["payment_alerts_enabled"] = request.payment_alerts_enabled
    if request.promotions_enabled is not None:
        update_data["promotions_enabled"] = request.promotions_enabled
        
    if not update_data:
        raise HTTPException(status_code=400, detail="No update data provided")
        
    # Update User Profile
    await db.users.update_one({"id": current_user.id}, {"$set": update_data})
    
    # Also update any customer records associated with this phone number
    if "name" in update_data:
        await db.customers.update_many(
            {"phone": current_user.phone},
            {"$set": {"name": update_data["name"]}}
        )
    
    updated_user = await db.users.find_one({"id": current_user.id})
    return User(**parse_from_mongo(updated_user))

@api_router.post("/auth/me/photo")
async def upload_profile_photo(request: ProfilePhotoRequest, current_user: User = Depends(get_current_user)):
    """Upload/update profile photo (base64 encoded)"""
    if not request.photo:
        raise HTTPException(status_code=400, detail="No photo data provided")
    
    # Validate base64 size (limit ~5MB of base64 data)
    if len(request.photo) > 7_000_000:
        raise HTTPException(status_code=400, detail="Photo is too large. Maximum size is 5MB.")
    
    await db.users.update_one(
        {"id": current_user.id},
        {"$set": {"profile_photo": request.photo}}
    )
    
    updated_user = await db.users.find_one({"id": current_user.id})
    return {"user": User(**parse_from_mongo(updated_user)).dict(), "message": "Profile photo updated successfully"}

@api_router.delete("/auth/me/photo")
async def remove_profile_photo(current_user: User = Depends(get_current_user)):
    """Remove current user's profile photo"""
    await db.users.update_one(
        {"id": current_user.id},
        {"$unset": {"profile_photo": ""}}
    )
    
    updated_user = await db.users.find_one({"id": current_user.id})
    return {"user": User(**parse_from_mongo(updated_user)).dict(), "message": "Profile photo removed successfully"}

# ==================== Security & Privacy Routes ====================

@api_router.get("/auth/sessions")
async def get_sessions(current_user: User = Depends(get_current_user)):
    """Get active sessions from database (unified by phone, backward compatible)"""
    sessions = await db.sessions.find({
        "$or": [
            {"phone": current_user.phone},
            {"user_id": current_user.id}
        ]
    }).to_list(length=None)
    
    # Format for frontend
    formatted_sessions = []
    for s in sessions:
        last_active = parse_from_mongo(s).get("last_active")
        # Simple "Last Active" formatting
        if last_active:
            diff = datetime.now(timezone.utc) - last_active.replace(tzinfo=timezone.utc)
            if diff.total_seconds() < 60:
                last_active_str = "Just now"
            elif diff.total_seconds() < 3600:
                last_active_str = f"{int(diff.total_seconds() / 60)} minutes ago"
            else:
                last_active_str = f"{int(diff.total_seconds() / 3600)} hours ago"
        else:
            last_active_str = "Unknown"

        formatted_sessions.append({
            "id": s["id"],
            "device": s["device"],
            "os": s["os"],
            "last_active": last_active_str
        })

    return {"sessions": formatted_sessions}

@api_router.post("/auth/logout")
async def logout_session(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Logout current session by deleting it from DB"""
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=["HS256"])
        session_id = payload.get("session_id")
        user_id = payload.get("user_id")
        
        if session_id and user_id:
            await db.sessions.delete_one({"id": session_id, "user_id": user_id})
            logger.info(f"Session {session_id} for user {user_id} deleted successfully.")
            
        return {"message": "Logged out successfully"}
    except:
        # Even if token is invalid, we return success as the goal is to be logged out
        return {"message": "Logged out"}

@api_router.post("/auth/logout-all")
async def logout_all_sessions(current_user: User = Depends(get_current_user)):
    """Logout all sessions for the current phone/user by deleting from DB"""
    await db.sessions.delete_many({
        "$or": [
            {"phone": current_user.phone},
            {"user_id": current_user.id}
        ]
    })
    return {"message": "Logged out from all sessions successfully"}

@api_router.post("/auth/request-data-export")
async def request_data_export(current_user: User = Depends(get_current_user)):
    """Request a data export for the current user"""
    # 1. Gather all user data
    user_data = current_user.dict()
    # 2. Check Role and Gather Data
    if current_user.active_role == "customer":
        # Customer: Find their profiles across all shops using phone number
        customers = await db.customers.find({"phone": current_user.phone}).to_list(length=None)
        customers_data = [parse_from_mongo(c) for c in customers]
        customer_ids = [c["id"] for c in customers_data]
        shop_ids = list(set([c["shop_id"] for c in customers_data]))
        
        # Get the shops they are customers of
        shops = await db.shops.find({"id": {"$in": shop_ids}}).to_list(length=None)
        shops_data = [parse_from_mongo(shop) for shop in shops]
        
        # Get ONLY their personal transactions
        transactions = await db.transactions.find({"customer_id": {"$in": customer_ids}}).to_list(length=None)
        transactions_data = [parse_from_mongo(t) for t in transactions]
    else:
        # Shopowner: Gather all shops owned by user
        shops = await db.shops.find({"owner_id": current_user.id}).to_list(length=None)
        shops_data = [parse_from_mongo(shop) for shop in shops]
        shop_ids = [shop["id"] for shop in shops_data]
        
        # Gather all customers in these shops
        customers = await db.customers.find({"shop_id": {"$in": shop_ids}}).to_list(length=None)
        customers_data = [parse_from_mongo(customer) for customer in customers]
        
        # Gather all transactions in these shops
        transactions = await db.transactions.find({"shop_id": {"$in": shop_ids}}).to_list(length=None)
        transactions_data = [parse_from_mongo(t) for t in transactions]
    
    # Calculate IST time (UTC + 5:30)
    ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    formatted_date = ist_now.strftime("%d-%m-%Y %I:%M %p")

    export_data = {
        "user_profile": user_data,
        "shops": shops_data,
        "customers": customers_data,
        "transactions": transactions_data,
        "export_date": formatted_date
    }
    
    # 5. Generate PDF Document
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    Story = []
    
    Story.append(Paragraph("XMunim - Data Export Report", styles['Title']))
    Story.append(Spacer(1, 12))
    
    Story.append(Paragraph(f"Generated on: {export_data['export_date']}", styles['Normal']))
    Story.append(Spacer(1, 12))
    
    Story.append(Paragraph("User Profile", styles['Heading2']))
    Story.append(Paragraph(f"Name: {user_data.get('name', 'N/A')}", styles['Normal']))
    Story.append(Paragraph(f"Phone: {user_data.get('phone', 'N/A')}", styles['Normal']))
    Story.append(Spacer(1, 12))
    
    # Build maps for lookups
    customer_map = {c.get('id'): c.get('name', 'Unknown') for c in customers_data}
    shop_map = {s.get('id'): s.get('name', 'Unknown Shop') for s in shops_data}
    
    is_customer = current_user.active_role == "customer"
    
    shops_title = "Connected Shops" if is_customer else "Shops Owned"
    Story.append(Paragraph(shops_title, styles['Heading2']))
    for shop in shops_data:
        Story.append(Paragraph(f"- {shop.get('name')} ({shop.get('category')})", styles['Normal']))
    Story.append(Spacer(1, 12))
    
    cust_title = "Shop Balances" if is_customer else "Customers"
    Story.append(Paragraph(cust_title, styles['Heading2']))
    
    # ------------------ CUSTOMERS/BALANCES TABLE ------------------
    col1_header = "Shop" if is_customer else "Name"
    cust_data_table = [[col1_header, "Phone", "Balance (Rs.)"]] # Header row
    
    for cust in customers_data:
        balance_val = cust.get('balance') or 0
        if is_customer:
            name_val = shop_map.get(cust.get('shop_id'), 'Unknown Shop')
        else:
            name_val = cust.get('name', 'N/A')
        cust_data_table.append([name_val, cust.get('phone', 'N/A'), f"{balance_val:.2f}"])
        
    if len(cust_data_table) > 1:
        c_table = Table(cust_data_table, colWidths=[200, 150, 100], hAlign='LEFT')
        c_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4A90E2')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F5F7FA')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#E1E8ED'))
        ]))
        Story.append(c_table)
    else:
        Story.append(Paragraph("No customers found.", styles['Normal']))
        
    Story.append(Spacer(1, 20))
    
    # ------------------ TRANSACTIONS TABLE ------------------
    Story.append(Paragraph("Transactions", styles['Heading2']))
    
    def build_tx_table(tx_list, title=None):
        if title:
            Story.append(Paragraph(title, styles['Heading3']))
            Story.append(Spacer(1, 6))
            
        col3_header = "Shop" if is_customer else "Customer"
        tx_data_table = [["Date", "Time", col3_header, "Item(s)", "Type", "Amount (Rs.)"]]
        
        # Sort transactions by date/time (newest first)
        sorted_tx = sorted(tx_list, key=lambda x: x.get('date', ''), reverse=True)
        
        for t in sorted_tx:
            raw_date = str(t.get('date', 'N/A')).replace('T', ' ')
            date_str, time_str = raw_date, ""
            
            parts = raw_date.split(' ')
            if parts:
                date_str = parts[0]
                # Strip the seconds and timezone, keeping just HH:MM
                time_str = parts[1][:5] if len(parts) > 1 else ""
                
            if is_customer:
                col3_val = shop_map.get(t.get('shop_id'), 'Unknown Shop')
            else:
                col3_val = customer_map.get(t.get('customer_id'), 'Unknown')
                
            if len(col3_val) > 15:
                col3_val = col3_val[:12] + "..."
                
            items = 'N/A'
            products = t.get('products')
            if products and isinstance(products, list) and len(products) > 0:
                items = ", ".join([str(p.get('name', '')) for p in products if isinstance(p, dict)])
            if items == 'N/A' and t.get('note'):
                items = str(t.get('note'))
                
            if len(items) > 20: 
                items = items[:17] + "..."
                
            raw_type = str(t.get('type', '')).lower()
            if raw_type == 'debit':
                tx_type = 'Payment'
            else:
                tx_type = raw_type.capitalize()
            amount_val = t.get('amount') or 0
            
            tx_data_table.append([date_str, time_str, col3_val, items, tx_type, f"{amount_val:.2f}"])
            
        if len(tx_data_table) > 1:
            t_table = Table(tx_data_table, colWidths=[65, 45, 90, 140, 60, 80], hAlign='LEFT')
            t_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4A90E2')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F5F7FA')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#E1E8ED'))
            ]))
            Story.append(t_table)
        else:
            Story.append(Paragraph("No transactions found.", styles['Normal']))
            
        Story.append(Spacer(1, 10))
        Story.append(Paragraph(f"<b>Total Count:</b> {len(tx_list)}", styles['Normal']))
        Story.append(Spacer(1, 15))

    # Build one unified table for either shop owner or customer
    build_tx_table(transactions_data)
    
    # Build PDF
    doc.build(Story)
    
    # Get PDF base64 string
    pdf_value = buffer.getvalue()
    buffer.close()
    pdf_base64 = base64.b64encode(pdf_value).decode('utf-8')
    
    # Return the base64 encoded PDF string back to the frontend
    return {
        "success": True, 
        "pdf_base64": pdf_base64,
        "message": "Data export generated successfully."
    }

@api_router.post("/auth/reset-pin")
async def reset_login_pin(current_user: User = Depends(get_current_user)):
    """Initiate a PIN reset for the user"""
    # Simply return a success message for the mock flow
    return {"message": "Reset PIN sent to " + current_user.phone}

# ==================== Admin Role Management ====================

@api_router.post("/admin/assign-role")
async def assign_user_role(request: AssignRoleRequest, current_user: User = Depends(get_admin_user)):
    """Assign or revoke admin roles to/from users"""
    can_assign_admin = "super_admin" in current_user.admin_roles
    
    valid_roles = ["admin", "super_admin"]
    invalid_roles = [role for role in request.admin_roles if role not in valid_roles]
    if invalid_roles:
        raise HTTPException(status_code=400, detail=f"Invalid roles: {invalid_roles}")
    
    if not can_assign_admin:
        raise HTTPException(status_code=403, detail="Only super admin can manage admin roles")
    
    target_user = await db.users.find_one({"id": request.user_id})
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    target_user_obj = User(**parse_from_mongo(target_user))
    
    if request.action == "grant":
        new_admin_roles = list(set(target_user_obj.admin_roles + request.admin_roles))
    elif request.action == "revoke":
        new_admin_roles = [role for role in target_user_obj.admin_roles if role not in request.admin_roles]
    else:
        raise HTTPException(status_code=400, detail="Action must be 'grant' or 'revoke'")
    
    await db.users.update_one(
        {"id": request.user_id},
        {"$set": {"admin_roles": new_admin_roles}}
    )
    
    if target_user_obj.active_role == "admin" and "admin" not in new_admin_roles and "super_admin" not in new_admin_roles:
        await db.users.update_one(
            {"id": request.user_id},
            {"$set": {"active_role": "customer"}}
        )
    
    updated_user = await db.users.find_one({"id": request.user_id})
    return {
        "user": User(**parse_from_mongo(updated_user)).dict(),
        "message": f"Roles {request.action}ed successfully"
    }

@api_router.post("/admin/promote-to-super-admin/{user_id}")
async def promote_to_super_admin(user_id: str, current_user: User = Depends(get_admin_user)):
    """Promote a user to super admin"""
    is_super_admin = "super_admin" in current_user.admin_roles
    
    if not is_super_admin:
        raise HTTPException(
            status_code=403,
            detail="Only super admin can promote users"
        )
    
    target_user = await db.users.find_one({"id": user_id})
    if not target_user:
        raise HTTPException(status_code=404, detail="User not found")
    
    target_user_obj = User(**parse_from_mongo(target_user))
    new_admin_roles = list(set(target_user_obj.admin_roles + ["super_admin", "admin"]))
    
    await db.users.update_one(
        {"id": user_id},
        {"$set": {"admin_roles": new_admin_roles}}
    )
    
    updated_user = await db.users.find_one({"id": user_id})
    return {
        "user": User(**parse_from_mongo(updated_user)).dict(),
        "message": "User promoted to super admin successfully"
    }

@api_router.get("/admin/users-for-role-assignment")
async def get_users_for_role_assignment(current_user: User = Depends(get_admin_user)):
    """Get all users for role assignment interface"""
    users = await db.users.find().to_list(length=None)
    
    # Get all shop owner IDs
    shops = await db.shops.find({}, {"owner_id": 1}).to_list(length=None)
    shop_owner_ids = set(shop["owner_id"] for shop in shops)
    
    users_data = []
    for user in users:
        user_obj = User(**parse_from_mongo(user))
        users_data.append({
            "id": user_obj.id,
            "name": user_obj.name,
            "phone": user_obj.phone,
            "active_role": user_obj.active_role,
            "admin_roles": user_obj.admin_roles,
            "has_shop": user_obj.id in shop_owner_ids,
            "created_at": user_obj.created_at,
            "verified": user_obj.verified
        })
    
    return {
        "users": users_data,
        "current_user_roles": current_user.admin_roles
    }

# ==================== Shop Owner Routes ====================

@api_router.post("/shops", response_model=Shop)
async def create_shop(request: ShopCreateRequest, current_user: User = Depends(get_current_user)):
    """Create a new shop"""
    if current_user.active_role != "shop_owner":
        raise HTTPException(status_code=403, detail="Only shop owners can create shops")
    
    shop = Shop(
        owner_id=current_user.id,
        name=request.name,
        category=request.category,
        pincode=request.pincode,
        city=request.city,
        area=request.area,
        state=request.state,
        country=request.country,
        location=request.location,
        gst_number=request.gst_number
    )
    
    shop_dict = prepare_for_mongo(shop.dict())
    await db.shops.insert_one(shop_dict)
    return shop

@api_router.get("/shops", response_model=List[Shop])
async def get_my_shops(current_user: User = Depends(get_current_user)):
    """Get shops owned by current user"""
    shops = await db.shops.find({"owner_id": current_user.id}).to_list(length=None)
    return [Shop(**parse_from_mongo(shop)) for shop in shops]

@api_router.put("/shops/{shop_id}", response_model=Shop)
async def update_shop(shop_id: str, request: ShopUpdateRequest, current_user: User = Depends(get_current_user)):
    """Update shop details"""
    # Verify ownership
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    update_data = {k: v for k, v in request.dict().items() if v is not None}
    
    if not update_data:
         raise HTTPException(status_code=400, detail="No update data provided")

    await db.shops.update_one({"id": shop_id}, {"$set": update_data})
    
    updated_shop = await db.shops.find_one({"id": shop_id})
    return Shop(**parse_from_mongo(updated_shop))

@api_router.get("/shops/{shop_id}/customers")
async def get_shop_customers(shop_id: str, current_user: User = Depends(get_current_user), from_date: Optional[str] = None, to_date: Optional[str] = None):
    """Get customers for a specific shop with enriched transaction data"""
    query = {"id": shop_id}
    if current_user.active_role != "admin":
        query["owner_id"] = current_user.id
        
    shop = await db.shops.find_one(query)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    customers = await db.customers.find({"shop_id": shop_id}).to_list(length=None)
    
    # All-time stats for the summary cards
    total_customers_count = len(customers)
    all_time_with_dues_count = 0
    all_time_total_dues = 0
    for c in customers:
        bal = c.get("balance", 0) or 0
        if bal < 0:
            all_time_with_dues_count += 1
            all_time_total_dues += abs(bal)
    
    # Date Filter setup
    tx_query = {"shop_id": shop_id}
    date_filter = None
    if from_date or to_date:
        date_filter = {}
        try:
            if from_date:
                # Convert string to datetime for BSON query
                d_from = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
                date_filter["$gte"] = d_from
            if to_date:
                # Convert string to datetime for BSON query
                d_to_str = to_date if 'T' in to_date else to_date + "T23:59:59.999Z"
                d_to = datetime.fromisoformat(d_to_str.replace('Z', '+00:00'))
                date_filter["$lte"] = d_to
            tx_query["date"] = date_filter
        except Exception as e:
            logger.error(f"Date parsing error in get_shop_customers: {e}")
    
    # Period-specific stats - Fetch ALL relevant transactions once
    all_transactions = await db.transactions.find(tx_query).sort("date", -1).to_list(length=None)
    
    def safe_amt(tx):
        try:
            return float(tx.get("amount", 0))
        except (TypeError, ValueError):
            return 0.0

    period_sales = 0.0
    period_payments = 0.0
    
    # Dictionaries to avoid N+1 queries
    customer_period_deltas = {}
    active_customer_ids = set()
    customer_transactions_map = {} # cid -> list of transactions
    
    for tx in all_transactions:
        cid = str(tx.get("customer_id", ""))
        if not cid: continue
        
        active_customer_ids.add(cid)
        amt = safe_amt(tx)
        tx_type = str(tx.get("type", "")).lower()
        
        # Track transactions per customer
        if cid not in customer_transactions_map:
            customer_transactions_map[cid] = []
        customer_transactions_map[cid].append(tx)
        
        if tx_type == "credit":
            period_sales += amt
            customer_period_deltas[cid] = customer_period_deltas.get(cid, 0) - amt
        elif tx_type in ["payment", "debit"]:
            period_payments += amt
            customer_period_deltas[cid] = customer_period_deltas.get(cid, 0) + amt

    # Enrichment loop for list cards
    customers_with_details = []
    for customer in customers:
        c_id = str(customer.get("id", ""))
        # If date filter active, skip customers with no transactions in the range
        if (from_date or to_date) and c_id not in active_customer_ids:
            continue

        try:
            customer_data = Customer(**parse_from_mongo(customer)).dict()
            
            # Add period specific delta
            customer_data["period_delta"] = customer_period_deltas.get(c_id, 0)
            
            # Use pre-fetched transactions to avoid N+1 queries
            txs_card = customer_transactions_map.get(c_id, [])
            customer_data["total_transactions"] = len(txs_card)
            customer_data["last_transaction_date"] = txs_card[0]["date"] if txs_card else None
            
            customers_with_details.append(customer_data)
        except Exception as e:
            logger.error(f"Skipping corrupted customer {c_id}: {e}")
            continue
    
    return {
        "customers": customers_with_details,
        "total_customers": total_customers_count,
        "all_time_with_dues": all_time_with_dues_count,
        "all_time_total_dues": all_time_total_dues,
        "period_sales": period_sales,
        "period_payments": period_payments,
        "period_transactions": len(all_transactions),
        "period_active_customers": len(active_customer_ids),
        # Backward compatibility fields
        "total_amount": period_sales + period_payments,
        "total_sales": period_sales,
        "total_dues": all_time_total_dues,
        "with_dues": all_time_with_dues_count,
        "total_transactions": len(all_transactions)
    }

@api_router.post("/shops/{shop_id}/customers", response_model=Customer)
async def add_customer(shop_id: str, request: CustomerCreateRequest, current_user: User = Depends(get_current_user)):
    """Add a customer to a shop"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Business not found")
    
    # Check if the customer already exists in this specific shop with the same type
    existing_customer = await db.customers.find_one({"shop_id": shop_id, "phone": request.phone, "type": request.type or "customer"})
    if existing_customer:
        raise HTTPException(status_code=400, detail="User with this phone number already exists in this type")
    
    customer = Customer(
        shop_id=shop_id,
        name=request.name,
        phone=request.phone,
        nickname=request.nickname,
        type=request.type if hasattr(request, "type") and request.type else "customer",
        is_verified=False
    )
    
    
    customer_dict = prepare_for_mongo(customer.dict())
    await db.customers.insert_one(customer_dict)
    return customer

@api_router.get("/shops/{shop_id}/customers/{customer_id}", response_model=Customer)
async def get_customer(shop_id: str, customer_id: str, current_user: User = Depends(get_current_user)):
    """Get a specific customer's details"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    customer = await db.customers.find_one({"id": customer_id, "shop_id": shop_id})
    if not customer:
        # Fallback for legacy where ID might be MongoDB _id
        try:
            customer = await db.customers.find_one({"_id": ObjectId(customer_id), "shop_id": shop_id})
        except:
            pass
            
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    return Customer(**parse_from_mongo(customer))

@api_router.put("/shops/{shop_id}/customers/{customer_id}", response_model=Customer)
async def update_customer(shop_id: str, customer_id: str, request: CustomerUpdateRequest, current_user: User = Depends(get_current_user)):
    """Update customer details"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    customer = await db.customers.find_one({"id": customer_id, "shop_id": shop_id})
    if not customer:
        try:
            customer = await db.customers.find_one({"_id": ObjectId(customer_id), "shop_id": shop_id})
        except:
            pass
            
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    # Get all fields explicitly provided in the request (including nulls)
    request_data = request.dict(exclude_unset=True)
    if not request_data:
        raise HTTPException(status_code=400, detail="No update data provided")

    update_data = {}
    for key, value in request_data.items():
        if key == "phone":
            if customer.get("phone") != value:
                # Check if the new phone number already exists in this shop for the same type
                target_type = request_data.get("type", customer.get("type", "customer"))
                existing_customer = await db.customers.find_one({
                    "shop_id": shop_id, 
                    "phone": value, 
                    "type": target_type
                })
                if existing_customer:
                    raise HTTPException(status_code=400, detail="Phone number already exists for this category")
                
                update_data["phone"] = value
                update_data["is_verified"] = False
        else:
            update_data[key] = value

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid update data provided")

    target_filter = {"_id": customer["_id"]} if "_id" in customer else {"id": customer_id, "shop_id": shop_id}
    await db.customers.update_one(target_filter, {"$set": update_data})

    updated_customer = await db.customers.find_one(target_filter)
    return Customer(**parse_from_mongo(updated_customer))

@api_router.put("/shops/{shop_id}/customers/{customer_id}/service_data", response_model=Customer)
async def update_service_data(
    shop_id: str, 
    customer_id: str, 
    request: dict = Body(...),
    current_user: User = Depends(get_current_user)
):
    """Specific endpoint to update service rate and calendar logs for Staff/Services.
    Accepts keys: service_rate, service_rate_type, service_log OR date & status to update a single record.
    """
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    customer = await db.customers.find_one({"id": customer_id, "shop_id": shop_id})
    if not customer:
        raise HTTPException(status_code=404, detail="User not found")
        
    if not customer.get("is_verified", False):
        raise HTTPException(
            status_code=403, 
            detail="Customer is not verified. Please send verification link and have customer verify before updating service data."
        )

    if customer.get("type", "customer") not in ["services", "staff"]:
        raise HTTPException(status_code=400, detail="User is not a service or staff member")

    update_data = {}
    
    # Optional fields to update
    if "service_rate" in request:
        update_data["service_rate"] = request["service_rate"]
    if "service_rate_type" in request:
        update_data["service_rate_type"] = request["service_rate_type"]
        
    # Handle individual log updates or full replacements
    if "service_log" in request:
        update_data["service_log"] = request["service_log"]
    elif "date" in request and "status" in request:
        # Update just one specific date in the log
        current_log = customer.get("service_log", {}) or {}
        current_log[request["date"]] = request["status"]
        update_data["service_log"] = current_log

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid update data provided")

    target_filter = {"_id": customer["_id"]} if "_id" in customer else {"id": customer_id, "shop_id": shop_id}
    await db.customers.update_one(target_filter, {"$set": update_data})

    updated_customer = await db.customers.find_one(target_filter)
    return Customer(**parse_from_mongo(updated_customer))

# ==================== Service Routes ====================

@api_router.get("/shops/{shop_id}/services")
async def get_shop_services(shop_id: str, current_user: User = Depends(get_current_user)):
    """Get all services for a specific shop"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    services = await db.services.find({"shop_id": shop_id}).to_list(length=None)
    return [Service(**parse_from_mongo(s)) for s in services]

@api_router.post("/shops/{shop_id}/services", response_model=Service)
async def add_service(shop_id: str, request: ServiceCreateRequest, current_user: User = Depends(get_current_user)):
    """Add a service to a shop"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Business not found")
    
    # Check if the service already exists in this shop with the same name/phone? 
    # Usually phone is the unique key for people.
    existing_service = await db.services.find_one({"shop_id": shop_id, "phone": request.phone})
    if existing_service:
        raise HTTPException(status_code=400, detail="Service with this phone number already exists")
    
    service = Service(
        shop_id=shop_id,
        name=request.name,
        phone=request.phone,
        nickname=request.nickname,
        category=request.category,
        service_rate=request.service_rate,
        service_rate_type=request.service_rate_type,
        is_verified=False
    )
    
    service_dict = prepare_for_mongo(service.dict())
    await db.services.insert_one(service_dict)
    return service

@api_router.get("/shops/{shop_id}/services/{service_id}", response_model=Service)
async def get_service(shop_id: str, service_id: str, current_user: User = Depends(get_current_user)):
    """Get a specific service's details"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    service = await db.services.find_one({"id": service_id, "shop_id": shop_id})
    if not service:
        # Fallback for legacy where ID might be MongoDB _id
        try:
            from bson import ObjectId
            service = await db.services.find_one({"_id": ObjectId(service_id), "shop_id": shop_id})
        except:
            pass
            
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    return Service(**parse_from_mongo(service))

@api_router.put("/shops/{shop_id}/services/{service_id}", response_model=Service)
async def update_service(shop_id: str, service_id: str, request: ServiceUpdateRequest, current_user: User = Depends(get_current_user)):
    """Update service details"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    service = await db.services.find_one({"id": service_id, "shop_id": shop_id})
    if not service:
        try:
            from bson import ObjectId
            service = await db.services.find_one({"_id": ObjectId(service_id), "shop_id": shop_id})
        except:
            pass
            
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    # Get all fields explicitly provided in the request (including nulls)
    request_data = request.dict(exclude_unset=True)
    if not request_data:
        raise HTTPException(status_code=400, detail="No update data provided")

    update_data = {}
    for key, value in request_data.items():
        if key == "phone":
            if service.get("phone") != value:
                # Check for duplicates in same shop and category
                existing = await db.services.find_one({
                    "shop_id": shop_id,
                    "phone": value
                })
                if existing:
                    raise HTTPException(status_code=400, detail="Phone number already exists for another service")
                
                update_data["phone"] = value
                update_data["is_verified"] = False
        else:
            update_data[key] = value

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid update data provided")

    # Use the internal _id if the search was by _id
    target_filter = {"_id": service["_id"]} if "_id" in service else {"id": service_id, "shop_id": shop_id}
    await db.services.update_one(target_filter, {"$set": update_data})

    updated_service = await db.services.find_one(target_filter)
    return Service(**parse_from_mongo(updated_service))

@api_router.put("/shops/{shop_id}/services/{service_id}/service_data", response_model=Service)
async def update_service_attendance(
    shop_id: str, 
    service_id: str, 
    request: dict = Body(...),
    current_user: User = Depends(get_current_user)
):
    """Specific endpoint to update service rate and calendar logs for Services."""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    service = await db.services.find_one({"id": service_id, "shop_id": shop_id})
    if not service:
        try:
            from bson import ObjectId
            service = await db.services.find_one({"_id": ObjectId(service_id), "shop_id": shop_id})
        except:
            pass
            
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")
        
    # Same verification check as customers
    if not service.get("is_verified", False):
         raise HTTPException(
            status_code=403, 
            detail="Service is not verified. Please verify before updating attendance."
        )

    update_data = {}
    if "service_rate" in request:
        update_data["service_rate"] = request["service_rate"]
    if "service_rate_type" in request:
        update_data["service_rate_type"] = request["service_rate_type"]
    if "service_log" in request:
        update_data["service_log"] = request["service_log"]
    elif "date" in request and "status" in request:
        current_log = service.get("service_log", {}) or {}
        current_log[request["date"]] = request["status"]
        update_data["service_log"] = current_log

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid update data provided")

    target_filter = {"_id": service["_id"]} if "_id" in service else {"id": service_id, "shop_id": shop_id}
    await db.services.update_one(target_filter, {"$set": update_data})

    updated_service = await db.services.find_one(target_filter)
    return Service(**parse_from_mongo(updated_service))

@api_router.delete("/shops/{shop_id}/services/{service_id}")
async def delete_service(shop_id: str, service_id: str, current_user: User = Depends(get_current_user)):
    """Delete a service (soft delete not implemented here, hard delete)"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    # Try UUID look up first
    result = await db.services.delete_one({"id": service_id, "shop_id": shop_id})
    
    if result.deleted_count == 0:
        # Fallback to _id
        try:
            from bson import ObjectId
            result = await db.services.delete_one({"_id": ObjectId(service_id), "shop_id": shop_id})
        except:
            pass
            
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Service not found")
        
    return {"success": True, "message": "Service deleted successfully"}

@api_router.post("/shops/{shop_id}/services/{service_id}/notify-payment")
async def notify_service_payment(shop_id: str, service_id: str, request: PushNotificationRequest, current_user: User = Depends(get_current_user)):
    """Send a push notification to a service provider for payment request and log it"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    service = await db.services.find_one({"id": service_id, "shop_id": shop_id})
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    user = await db.users.find_one({"phone": service["phone"]})

    if (request.method or "Push Notification") == "Push Notification":
        if user:
            if not user.get("push_enabled", True):
                raise HTTPException(status_code=400, detail="Service provider has disabled push notifications.")
            if not user.get("payment_alerts_enabled", True):
                raise HTTPException(status_code=400, detail="Service provider has disabled payment alerts.")
    
    payment_req = PaymentRequest(
        shop_id=shop_id,
        customer_id=service_id, # Reusing customer_id field for target ID
        amount=abs(service.get("balance", 0)),
        method=request.method or "Push Notification",
        title=request.title,
        message=request.body,
        status="pending" if request.scheduled_at else "sent",
        scheduled_at=request.scheduled_at
    )
    
    await db.payment_requests.insert_one(prepare_for_mongo(payment_req.dict()))

    if request.scheduled_at:
        return {"success": True, "message": "Reminder scheduled successfully", "id": payment_req.id}

    if (request.method or "Push Notification") == "Push Notification":
        if not user:
            raise HTTPException(status_code=404, detail="Service provider has not registered on the app yet. Try SMS or WhatsApp.")
        
        if not user.get("fcm_token"):
             raise HTTPException(status_code=400, detail="Service provider registered but FCM token is missing.")

        try:
            logger.info(f"Sending Push to Service: Title='{request.title}', Body='{request.body}'")
            message = messaging.Message(
                notification=messaging.Notification(title=request.title, body=request.body),
                android=messaging.AndroidConfig(
                    notification=messaging.AndroidNotification(
                        color="#304FFE",
                        channel_id="default"
                    )
                ),
                data=(request.data or {}),
                token=user["fcm_token"],
            )
            messaging.send(message)
            return {"success": True, "message": "Push notification sent", "id": payment_req.id}
        except Exception as e:
            logger.error(f"FCM Send Error: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to send push: {str(e)}")

    return {"success": True, "message": f"{request.method} request logged", "id": payment_req.id}

@api_router.get("/shops/{shop_id}/services/{service_id}/notifications")
async def get_service_payment_history(shop_id: str, service_id: str, current_user: User = Depends(get_current_user)):
    """Get payment request history for a specific service in a shop"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found or access denied")

    notifications = await db.payment_requests.find(
        {"shop_id": shop_id, "customer_id": service_id},
        sort=[("created_at", -1)]
    ).to_list(length=None)

    return [parse_from_mongo(noti) for noti in notifications]

# ==================== Staff Routes ====================

@api_router.get("/shops/{shop_id}/staff", response_model=List[Staff])
async def get_shop_staff(shop_id: str, current_user: User = Depends(get_current_user)):
    """Get all staff members for a specific shop"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    staff_members = await db.staff.find({"shop_id": shop_id}).to_list(length=None)
    return [Staff(**parse_from_mongo(s)) for s in staff_members]

@api_router.post("/shops/{shop_id}/staff", response_model=Staff)
async def add_staff(shop_id: str, request: StaffCreateRequest, current_user: User = Depends(get_current_user)):
    """Add a staff member to a shop"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Business not found")
    
    existing_staff = await db.staff.find_one({"shop_id": shop_id, "phone": request.phone})
    if existing_staff:
        raise HTTPException(status_code=400, detail="Staff member with this phone number already exists")
    
    staff = Staff(
        shop_id=shop_id,
        name=request.name,
        phone=request.phone,
        nickname=request.nickname,
        role=request.role,
        service_rate=request.service_rate,
        service_rate_type=request.service_rate_type,
        upi_id=request.upi_id,
        is_verified=False
    )
    
    staff_dict = prepare_for_mongo(staff.dict())
    await db.staff.insert_one(staff_dict)
    return staff

@api_router.get("/shops/{shop_id}/staff/{staff_id}", response_model=Staff)
async def get_staff(shop_id: str, staff_id: str, current_user: User = Depends(get_current_user)):
    """Get a specific staff member's details"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    staff = await db.staff.find_one({"id": staff_id, "shop_id": shop_id})
    if not staff:
        try:
            from bson import ObjectId
            staff = await db.staff.find_one({"_id": ObjectId(staff_id), "shop_id": shop_id})
        except:
            pass
            
    if not staff:
        raise HTTPException(status_code=404, detail="Staff not found")

    return Staff(**parse_from_mongo(staff))

@api_router.put("/shops/{shop_id}/staff/{staff_id}", response_model=Staff)
async def update_staff(shop_id: str, staff_id: str, request: StaffUpdateRequest, current_user: User = Depends(get_current_user)):
    """Update staff details"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    staff = await db.staff.find_one({"id": staff_id, "shop_id": shop_id})
    if not staff:
        try:
            from bson import ObjectId
            staff = await db.staff.find_one({"_id": ObjectId(staff_id), "shop_id": shop_id})
        except:
            pass
            
    if not staff:
        raise HTTPException(status_code=404, detail="Staff not found")

    # Get all fields explicitly provided in the request (including nulls)
    request_data = request.dict(exclude_unset=True)
    if not request_data:
        raise HTTPException(status_code=400, detail="No update data provided")

    update_data = {}
    for key, value in request_data.items():
        if key == "phone":
            if staff.get("phone") != value:
                # Check for duplicates in same shop and category
                existing = await db.staff.find_one({
                    "shop_id": shop_id,
                    "phone": value
                })
                if existing:
                    raise HTTPException(status_code=400, detail="Phone number already exists for another staff member")
                
                update_data["phone"] = value
                update_data["is_verified"] = False
        else:
            update_data[key] = value

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid update data provided")

    target_filter = {"_id": staff["_id"]} if "_id" in staff else {"id": staff_id, "shop_id": shop_id}
    await db.staff.update_one(target_filter, {"$set": update_data})

    updated_staff = await db.staff.find_one(target_filter)
    return Staff(**parse_from_mongo(updated_staff))

@api_router.put("/shops/{shop_id}/staff/{staff_id}/service_data", response_model=Staff)
async def update_staff_attendance(
    shop_id: str, 
    staff_id: str, 
    request: dict = Body(...),
    current_user: User = Depends(get_current_user)
):
    """Specific endpoint to update service rate and calendar logs for Staff."""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    staff = await db.staff.find_one({"id": staff_id, "shop_id": shop_id})
    if not staff:
        try:
            from bson import ObjectId
            staff = await db.staff.find_one({"_id": ObjectId(staff_id), "shop_id": shop_id})
        except:
            pass
            
    if not staff:
        raise HTTPException(status_code=404, detail="Staff not found")
        
    if not staff.get("is_verified", False):
         raise HTTPException(
            status_code=403, 
            detail="Staff is not verified. Please verify before updating attendance."
        )

    update_data = {}
    if "service_rate" in request:
        update_data["service_rate"] = request["service_rate"]
    if "service_rate_type" in request:
        update_data["service_rate_type"] = request["service_rate_type"]
    if "service_log" in request:
        update_data["service_log"] = request["service_log"]
    elif "date" in request and "status" in request:
        current_log = staff.get("service_log", {}) or {}
        current_log[request["date"]] = request["status"]
        update_data["service_log"] = current_log

    if not update_data:
        raise HTTPException(status_code=400, detail="No valid update data provided")

    target_filter = {"_id": staff["_id"]} if "_id" in staff else {"id": staff_id, "shop_id": shop_id}
    await db.staff.update_one(target_filter, {"$set": update_data})

    updated_staff = await db.staff.find_one(target_filter)
    return Staff(**parse_from_mongo(updated_staff))

@api_router.delete("/shops/{shop_id}/staff/{staff_id}")
async def delete_staff(shop_id: str, staff_id: str, current_user: User = Depends(get_current_user)):
    """Delete a staff member"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    result = await db.staff.delete_one({"id": staff_id, "shop_id": shop_id})
    if result.deleted_count == 0:
        try:
            from bson import ObjectId
            result = await db.staff.delete_one({"_id": ObjectId(staff_id), "shop_id": shop_id})
        except:
            pass
            
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Staff not found")
        
    return {"success": True, "message": "Staff deleted successfully"}

@api_router.post("/shops/{shop_id}/customers/{customer_id}/notify-payment")
async def notify_customer_payment(shop_id: str, customer_id: str, request: PushNotificationRequest, current_user: User = Depends(get_current_user)):
    """Send a push notification to a customer for payment request and log it"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    customer = await db.customers.find_one({"id": customer_id, "shop_id": shop_id})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    # Find the User document associated with this customer's phone number to get FCM token
    user = await db.users.find_one({"phone": customer["phone"]})

    # NEW: Check if customer has disabled notifications in their preferences
    if (request.method or "Push Notification") == "Push Notification":
        if user:
            if not user.get("push_enabled", True):
                raise HTTPException(status_code=400, detail="Customer has disabled push notifications in their preferences.")
            
            if not user.get("payment_alerts_enabled", True):
                raise HTTPException(status_code=400, detail="Customer has disabled payment alerts in their preferences.")
    
    # Logic for logging the request regardless of method (SMS/WhatsApp are handled client-side but can be logged via this endpoint)
    payment_req = PaymentRequest(
        shop_id=shop_id,
        customer_id=customer_id,
        amount=abs(customer.get("balance", 0)),
        method=request.method or "Push Notification",
        title=request.title,
        message=request.body,
        status="pending" if request.scheduled_at else "sent",
        scheduled_at=request.scheduled_at
    )
    
    # Store the record
    await db.payment_requests.insert_one(prepare_for_mongo(payment_req.dict()))

    if request.scheduled_at:
        return {"success": True, "message": "Reminder scheduled successfully", "id": payment_req.id}

    if (request.method or "Push Notification") == "Push Notification":
        if not user:
            raise HTTPException(status_code=404, detail="Customer has not registered on the app yet. Try SMS or WhatsApp.")
        
        if not user.get("fcm_token"):
            raise HTTPException(status_code=400, detail="Customer has not enabled push notifications.")

        logger.info(f"Sending Push: Title='{request.title}', Body='{request.body}' to token '...{user['fcm_token'][-10:]}'")
        
        # Send Firebase Push Notification
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=request.title,
                    body=request.body,
                ),
                android=messaging.AndroidConfig(
                    notification=messaging.AndroidNotification(
                        color="#304FFE",
                        channel_id="default"
                    )
                ),
                data=request.data or {},
                token=user["fcm_token"],
            )
            response = messaging.send(message)
            return {"success": True, "message_id": response, "message": "Notification sent successfully"}
        except Exception as e:
            print(f"Error sending push notification: {e}")
            # Update status to failed
            await db.payment_requests.update_one({"id": payment_req.id}, {"$set": {"status": "failed"}})
            raise HTTPException(status_code=500, detail=f"Failed to send push notification: {str(e)}")
    
    return {"success": True, "message": f"{request.method} request logged successfully"}

@api_router.post("/shops/{shop_id}/services/{service_id}/notify-payment")
async def notify_service_payment(shop_id: str, service_id: str, request: PushNotificationRequest, current_user: User = Depends(get_current_user)):
    """Send a push notification to a service for payment request and log it"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    service = await db.services.find_one({"id": service_id, "shop_id": shop_id})
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    # Find the User document associated with this service's phone number to get FCM token
    user = await db.users.find_one({"phone": service["phone"]})

    if (request.method or "Push Notification") == "Push Notification":
        if user:
            if not user.get("push_enabled", True):
                raise HTTPException(status_code=400, detail="User has disabled push notifications in their preferences.")
            
            if not user.get("payment_alerts_enabled", True):
                raise HTTPException(status_code=400, detail="User has disabled payment alerts in their preferences.")
    
    # Logic for logging the request
    payment_req = PaymentRequest(
        shop_id=shop_id,
        customer_id=service_id, # Using customer_id field for service_id in payment_requests collection
        amount=abs(service.get("balance", 0)),
        method=request.method or "Push Notification",
        title=request.title,
        message=request.body,
        status="pending" if request.scheduled_at else "sent",
        scheduled_at=request.scheduled_at
    )
    
    # Store the record
    await db.payment_requests.insert_one(prepare_for_mongo(payment_req.dict()))

    if request.scheduled_at:
        return {"success": True, "message": "Reminder scheduled successfully", "id": payment_req.id}

    if (request.method or "Push Notification") == "Push Notification":
        if not user:
            raise HTTPException(status_code=404, detail="User has not registered on the app yet. Try SMS or WhatsApp.")
        
        if not user.get("fcm_token"):
            raise HTTPException(status_code=400, detail="User has not enabled push notifications.")

        logger.info(f"Sending Push to Service: Title='{request.title}', Body='{request.body}' to token '...{user['fcm_token'][-10:]}'")
        
        # Send Firebase Push Notification
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=request.title,
                    body=request.body,
                ),
                android=messaging.AndroidConfig(
                    notification=messaging.AndroidNotification(
                        color="#304FFE",
                        channel_id="default"
                    )
                ),
                data=request.data or {},
                token=user["fcm_token"],
            )
            response = messaging.send(message)
            return {"success": True, "message_id": response, "message": "Notification sent successfully"}
        except Exception as e:
            print(f"Error sending push notification: {e}")
            await db.payment_requests.update_one({"id": payment_req.id}, {"$set": {"status": "failed"}})
            raise HTTPException(status_code=500, detail=f"Failed to send push notification: {str(e)}")
    
    return {"success": True, "message": f"{request.method} request logged successfully"}

@api_router.post("/shops/{shop_id}/notify-owner")
async def notify_shop_owner(shop_id: str, request: PushNotificationRequest, current_user: User = Depends(get_current_user)):
    """Send a push notification from staff/customer to the shop owner"""
    shop = await db.shops.find_one({"id": shop_id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    owner = await db.users.find_one({"id": shop["owner_id"]})
    if not owner:
        raise HTTPException(status_code=404, detail="Owner not found")

    if (request.method or "Push Notification") == "Push Notification":
        if not owner.get("push_enabled", True):
            raise HTTPException(status_code=400, detail="Shop owner has disabled push notifications in their preferences.")
        
        if not owner.get("payment_alerts_enabled", True):
            raise HTTPException(status_code=400, detail="Shop owner has disabled payment alerts in their preferences.")
            
        if not owner.get("fcm_token"):
            raise HTTPException(status_code=400, detail="Shop owner has not enabled push notifications on their device.")

        logger.info(f"Sending Push to Owner: Title='{request.title}', Body='{request.body}' to token '...{owner['fcm_token'][-10:]}'")
        
        try:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=request.title,
                    body=request.body,
                ),
                android=messaging.AndroidConfig(
                    notification=messaging.AndroidNotification(
                        color="#304FFE",
                        channel_id="default"
                    )
                ),
                data=request.data or {},
                token=owner["fcm_token"],
            )
            response = messaging.send(message)
            return {"success": True, "message_id": response, "message": "Notification sent to shop owner successfully"}
        except Exception as e:
            print(f"Error sending push notification to owner: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to send push notification: {str(e)}")
            
    return {"success": True, "message": f"{request.method} request sent successfully"}

@api_router.get("/shops/{shop_id}/services/{service_id}/notifications")
async def get_service_notifications(shop_id: str, service_id: str, current_user: User = Depends(get_current_user)):
    """Get notification history for a specific service"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    # Use customer_id field in payment_requests which stores service_id/customer_id
    notifications = await db.payment_requests.find({
        "shop_id": shop_id,
        "customer_id": service_id
    }).sort("created_at", -1).to_list(length=100)
    
    return [PaymentRequest(**parse_from_mongo(n)) for n in notifications]

@api_router.post("/shops/{shop_id}/services/{service_id}/send-verification")
async def send_service_verification_link(shop_id: str, service_id: str, current_user: User = Depends(get_current_user)):
    """Generate a verification link for a service"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    service = await db.services.find_one({"id": service_id, "shop_id": shop_id})
    if not service:
        raise HTTPException(status_code=404, detail="Service not found")

    # Reuse the same public verification endpoint, it will handle both collections
    verification_link = f"https://xmunim-backend.onrender.com/api/public/verify-customer/{service_id}"
    
    return {
        "success": True, 
        "verification_link": verification_link,
        "message": "Verification link generated successfully"
    }

@api_router.post("/shops/{shop_id}/customers/{customer_id}/send-verification")
async def send_verification_link(shop_id: str, customer_id: str, current_user: User = Depends(get_current_user)):
    """Generate a verification link for a customer"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    customer = await db.customers.find_one({"id": customer_id, "shop_id": shop_id})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    # Reuse the same public verification endpoint
    verification_link = f"https://xmunim-backend.onrender.com/api/public/verify-customer/{customer_id}"
    
    return {
        "success": True, 
        "verification_link": verification_link,
        "message": "Verification link generated successfully"
    }

@api_router.post("/shops/{shop_id}/staff/{staff_id}/send-verification")
async def send_staff_verification_link(shop_id: str, staff_id: str, current_user: User = Depends(get_current_user)):
    """Generate a verification link for a staff member"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    staff = await db.staff.find_one({"id": staff_id, "shop_id": shop_id})
    if not staff:
        raise HTTPException(status_code=404, detail="Staff not found")

    # Reuse the same public verification endpoint
    verification_link = f"https://xmunim-backend.onrender.com/api/public/verify-customer/{staff_id}"
    
    return {
        "success": True, 
        "verification_link": verification_link,
        "message": "Verification link generated successfully"
    }

    # In a real app, this would be a deep link to the app or a hosted page
    # For MVP, we'll return a link to our public verification endpoint
    # We use a hardcoded base URL for now as per implementation plan
    verification_link = f"https://xmunim-backend.onrender.com/api/public/verify-customer/{customer_id}"
    
    return {
        "success": True, 
        "verification_link": verification_link,
        "message": "Verification link generated successfully"
    }

@api_router.get("/public/verify-customer/{customer_id}")
async def view_verify_customer(customer_id: str):
    """Public endpoint to view the customer verification page"""
    logger.info(f"Verification page requested for account: {customer_id}")
    try:
        # Check customers collection first
        customer = await db.customers.find_one({"id": customer_id})
        collection_name = "customers"
        
        # If not found, check services collection
        if not customer:
            customer = await db.services.find_one({"id": customer_id})
            collection_name = "services"
            
        # If still not found, check staff collection
        if not customer:
            customer = await db.staff.find_one({"id": customer_id})
            collection_name = "staff"
            
        if not customer:
            logger.warning(f"Account not found for verification: {customer_id}")
            return HTMLResponse(
                content="""
                <html>
                    <body style="font-family: sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; background-color: #fce8e8;">
                        <div style="text-align: center; background: white; padding: 40px; border-radius: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                            <h1 style="color: #c53030; margin-bottom: 10px;">Customer Not Found</h1>
                            <p style="color: #666;">We couldn't find a customer with that ID.</p>
                        </div>
                    </body>
                </html>
                """, 
                status_code=404
            )
        
        shop = await db.shops.find_one({"id": customer.get("shop_id")})
        shop_name = shop.get("name", "XMunim") if shop else "XMunim"
        logger.info(f"Found customer {customer.get('name')} for shop {shop_name}")
        
        html_template = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Account Verification - XMunim</title>
            <style>
                * { box-sizing: border-box; margin: 0; padding: 0; }
                body {
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    min-height: 100vh;
                    background: linear-gradient(135deg, #EEF2FF 0%, #F3F4F6 50%, #ECFDF5 100%);
                }
                .container {
                    background-color: #FFFFFF;
                    border-radius: 24px;
                    padding: 40px 24px;
                    box-shadow: 0 20px 60px -12px rgba(0, 0, 0, 0.12);
                    text-align: center;
                    max-width: 420px;
                    width: 92%;
                }
                .icon-circle {
                    width: 88px;
                    height: 88px;
                    border-radius: 50%;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 42px;
                    margin: 0 auto 24px auto;
                    transition: all 0.5s ease;
                }
                .icon-neutral {
                    background: linear-gradient(135deg, #E5E7EB 0%, #D1D5DB 100%);
                    color: #6B7280;
                }
                .icon-success {
                    background: linear-gradient(135deg, #A7F3D0 0%, #6EE7B7 100%);
                    color: #059669;
                }
                h1 {
                    color: #111827;
                    font-size: 24px;
                    margin: 0 0 8px 0;
                    font-weight: 700;
                }
                .desc {
                    color: #6B7280;
                    font-size: 15px;
                    line-height: 1.5;
                    margin-bottom: 24px;
                }
                .shop-box {
                    background-color: #F9FAFB;
                    border: 1px solid #E5E7EB;
                    border-radius: 14px;
                    padding: 16px;
                    margin-bottom: 28px;
                }
                .shop-label {
                    font-size: 11px;
                    color: #9CA3AF;
                    text-transform: uppercase;
                    letter-spacing: 0.08em;
                    margin-bottom: 4px;
                    display: block;
                }
                .shop-name {
                    font-size: 18px;
                    color: #1F2937;
                    font-weight: 600;
                }
                .verified-badge {
                    display: none;
                    align-items: center;
                    justify-content: center;
                    gap: 6px;
                    margin-top: 10px;
                    padding: 8px 16px;
                    background: linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%);
                    border: 1px solid #6EE7B7;
                    border-radius: 10px;
                    color: #059669;
                    font-size: 14px;
                    font-weight: 600;
                }
                .verified-badge.show {
                    display: flex;
                }
                .verified-badge .badge-icon {
                    font-size: 16px;
                }
                .shop-box.verified {
                    border-color: #6EE7B7;
                    background-color: #F0FDF9;
                }
                .btn {
                    display: block;
                    width: 100%;
                    padding: 14px 24px;
                    border-radius: 12px;
                    font-weight: 600;
                    font-size: 15px;
                    border: none;
                    cursor: pointer;
                    font-family: inherit;
                    text-align: center;
                    text-decoration: none;
                    transition: transform 0.15s ease, box-shadow 0.15s ease;
                }
                .btn:active { transform: scale(0.97); }
                .btn-primary {
                    background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%);
                    color: white;
                    box-shadow: 0 4px 14px -3px rgba(59, 130, 246, 0.5);
                    margin-bottom: 12px;
                }
                .btn-success {
                    background: linear-gradient(135deg, #10B981 0%, #059669 100%);
                    color: white;
                    box-shadow: 0 4px 14px -3px rgba(16, 185, 129, 0.5);
                }
                .btn-dark {
                    background: linear-gradient(135deg, #1F2937 0%, #111827 100%);
                    color: white;
                    padding: 12px 16px;
                    font-size: 13px;
                    flex: 1;
                }
                .section-label {
                    font-size: 13px;
                    color: #9CA3AF;
                    margin: 20px 0 10px 0;
                }
                .divider {
                    height: 1px;
                    background-color: #F3F4F6;
                    margin: 20px 0;
                }
                .store-row {
                    display: flex;
                    gap: 10px;
                    margin-top: 10px;
                }
                .footer {
                    color: #D1D5DB;
                    font-size: 13px;
                    margin-top: 28px;
                }
                .footer strong { color: #9CA3AF; }

                /* Custom Modal */
                .modal-overlay {
                    display: none;
                    position: fixed;
                    top: 0; left: 0; right: 0; bottom: 0;
                    background: rgba(0,0,0,0.4);
                    z-index: 100;
                    align-items: center;
                    justify-content: center;
                }
                .modal-overlay.active { display: flex; }
                .modal-box {
                    background: white;
                    border-radius: 20px;
                    padding: 28px 24px;
                    max-width: 320px;
                    width: 85%;
                    text-align: center;
                    box-shadow: 0 25px 60px -12px rgba(0,0,0,0.25);
                }
                .modal-title {
                    font-size: 17px;
                    font-weight: 700;
                    color: #111827;
                    margin-bottom: 8px;
                }
                .modal-msg {
                    font-size: 15px;
                    color: #6B7280;
                    line-height: 1.5;
                    margin-bottom: 20px;
                }
                .modal-ok-btn {
                    background: linear-gradient(135deg, #3B82F6, #2563EB);
                    color: white;
                    border: none;
                    padding: 12px 40px;
                    border-radius: 10px;
                    font-weight: 600;
                    font-size: 15px;
                    cursor: pointer;
                    font-family: inherit;
                }
            </style>
            <script>
                
                function showModal(msg) {
                    document.getElementById('modal-message').innerText = msg;
                    document.getElementById('custom-modal').classList.add('active');
                }

                function closeModal() {
                    document.getElementById('custom-modal').classList.remove('active');
                }

                function showComingSoon(platform) {
                    showModal(platform + " app is coming very soon! Stay tuned.");
                }

                async function verifyInBrowser() {
                    var btn = document.querySelector('.btn-success');
                    btn.disabled = true;
                    btn.innerText = 'Verifying...';
                    
                    try {
                        var response = await fetch('/api/public/verify-customer/{{CUSTOMER_ID}}', {
                            method: 'POST'
                        });
                        var data = await response.json();
                        
                        if (data.success || data.message === "Already verified") {
                            document.getElementById('icon-circle').className = 'icon-circle icon-success';
                            document.getElementById('icon-circle').innerText = '✓';
                            document.getElementById('header-title').innerText = 'Verification Successful!';
                            document.getElementById('desc-text').innerText = 'Your Customer account has been successfully verified.';
                            document.getElementById('action-buttons').style.display = 'none';
                        } else {
                            showModal("Verification failed. Please try again.");
                            btn.disabled = false;
                            btn.innerText = 'Verify in Browser';
                        }
                    } catch (error) {
                        showModal("Verification failed. Please try again.");
                        btn.disabled = false;
                        btn.innerText = 'Verify in Browser';
                    }
                }
            </script>
        </head>
        <body>
            <!-- Custom Modal (replaces alert) -->
            <div id="custom-modal" class="modal-overlay">
                <div class="modal-box" onclick="event.stopPropagation()">
                    <div class="modal-title">XMunim App</div>
                    <div id="modal-message" class="modal-msg"></div>
                    <button class="modal-ok-btn" onclick="closeModal()">OK</button>
                </div>
            </div>

            <div class="container">
                <div id="icon-circle" class="icon-circle icon-neutral">!</div>
                <h1 id="header-title">Verify Account</h1>
                <p id="desc-text" class="desc">Open the app to verify your customer account, or verify here in the browser.</p>
                
                <div id="shop-box" class="shop-box">
                    <span class="shop-label">Verify For</span>
                    <span class="shop-name">Shop {{SHOP_NAME}}</span>
                </div>
                
                <div id="action-buttons">                    
                    <div class="divider"></div>
                    <p class="section-label">Or verify immediately</p>
                    <button onclick="verifyInBrowser()" class="btn btn-success">Verify in Browser</button>
                    
                    <div class="divider"></div>
                    <p class="section-label">Don't have the app?</p>
                    <div class="store-row">
                        <button onclick="showComingSoon('Google Play')" class="btn btn-dark">Google Play</button>
                        <button onclick="showComingSoon('App Store')" class="btn btn-dark">App Store</button>
                    </div>
                </div>
                
                <div class="footer">
                    Powered by <strong>XMunim</strong>
                </div>
            </div>
        </body>
        </html>
        """
        html_content = html_template.replace("{{CUSTOMER_ID}}", customer_id).replace("{{SHOP_NAME}}", shop_name)
        return HTMLResponse(content=html_content)
    except Exception as e:
        logger.error(f"Error serving verification page: {e}")
        return HTMLResponse(content=f"<html><body><h1>Internal Server Error</h1><p>{str(e)}</p></body></html>", status_code=500)

@api_router.post("/public/verify-customer/{customer_id}")
async def do_verify_customer(customer_id: str):
    """Explicitly mark customer (or service) verified via API"""
    # Check customers collection first
    customer = await db.customers.find_one({"id": customer_id})
    collection = db.customers
    type_name = "Customer"
    
    if not customer:
        # Check services collection
        customer = await db.services.find_one({"id": customer_id})
        collection = db.services
        type_name = "Service Member"
        
    if not customer:
        # Check staff collection
        customer = await db.staff.find_one({"id": customer_id})
        collection = db.staff
        type_name = "Staff Member"
        
    if not customer:
        raise HTTPException(status_code=404, detail="Account not found")
    
    shop = await db.shops.find_one({"id": customer.get("shop_id")})
    shop_name = shop.get("name", "XMunim") if shop else "XMunim"
    
    if customer.get("is_verified", False):
        return {"success": True, "message": "Already verified", "shop_name": shop_name}
        
    await collection.update_one(
        {"id": customer_id},
        {"$set": {"is_verified": True}}
    )
    return {"success": True, "message": f"{type_name} successfully verified", "shop_name": shop_name}

@api_router.post("/shops/{shop_id}/transactions", response_model=Transaction)
async def create_transaction(shop_id: str, request: TransactionCreateRequest, current_user: User = Depends(get_current_user)):
    """Create a new transaction"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    customer = await db.customers.find_one({"id": request.customer_id, "shop_id": shop_id})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    if not customer.get("is_verified", False):
        raise HTTPException(
            status_code=403, 
            detail="Customer is not verified. Please send verification link and have customer verify before adding transactions."
        )
    
    transaction_products = []
    calculated_amount = 0
    
    if request.products:
        for product_req in request.products:
            product = await db.products.find_one({"id": product_req.product_id, "shop_id": shop_id, "active": True})
            if not product:
                raise HTTPException(status_code=404, detail=f"Product not found: {product_req.product_id}")
            
            subtotal = product["price"] * product_req.quantity
            calculated_amount += subtotal
            
            transaction_products.append(TransactionProduct(
                product_id=product_req.product_id,
                name=product["name"],
                price=product["price"],
                quantity=product_req.quantity,
                subtotal=subtotal
            ))
    
    final_amount = calculated_amount if request.products else request.amount
    if final_amount is None:
        raise HTTPException(status_code=400, detail="Amount is required when no products are provided")
    
    transaction = Transaction(
        shop_id=shop_id,
        customer_id=request.customer_id,
        type=request.type,
        amount=final_amount,
        products=transaction_products,
        note=request.note
    )
    
    transaction_dict = prepare_for_mongo(transaction.dict())
    await db.transactions.insert_one(transaction_dict)
    
    balance_change = -final_amount if request.type == "credit" else final_amount
    await db.customers.update_one(
        {"id": request.customer_id},
        {"$inc": {"balance": balance_change}}
    )
    
    return transaction

@api_router.get("/shops/{shop_id}/transactions", response_model=List[Transaction])
async def get_shop_transactions(shop_id: str, current_user: User = Depends(get_current_user)):
    """Get transactions for a shop"""
    query = {"id": shop_id}
    if current_user.active_role != "admin":
        query["owner_id"] = current_user.id
        
    shop = await db.shops.find_one(query)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    transactions = await db.transactions.find({"shop_id": shop_id}).sort("created_at", -1).to_list(length=None)
    return [Transaction(**parse_from_mongo(transaction)) for transaction in transactions]

# ==================== Product Routes ====================

@api_router.post("/shops/{shop_id}/products", response_model=Product)
async def create_product(shop_id: str, request: ProductCreateRequest, current_user: User = Depends(get_current_user)):
    """Create a new product for a shop"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
        
    # Check for existing product with same name (case-insensitive) in the same shop
    existing_product = await db.products.find_one({
        "shop_id": shop_id, 
        "name": {"$regex": f"^{request.name}$", "$options": "i"}
    })
    
    if existing_product:
        raise HTTPException(status_code=400, detail="A product with this name already exists in this shop")
    
    product = Product(
        shop_id=shop_id,
        name=request.name,
        price=request.price
    )
    
    product_dict = prepare_for_mongo(product.dict())
    await db.products.insert_one(product_dict)
    return product

@api_router.get("/shops/{shop_id}/products", response_model=List[Product])
async def get_shop_products(shop_id: str, current_user: User = Depends(get_current_user)):
    """Get products for a specific shop"""
    query = {"id": shop_id}
    if current_user.active_role != "admin":
        query["owner_id"] = current_user.id
        
    shop = await db.shops.find_one(query)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    products = await db.products.find({"shop_id": shop_id, "active": True}).to_list(length=None)
    return [Product(**parse_from_mongo(product)) for product in products]

@api_router.put("/shops/{shop_id}/products/{product_id}", response_model=Product)
async def update_product(shop_id: str, product_id: str, request: ProductUpdateRequest, current_user: User = Depends(get_current_user)):
    """Update a product"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    product = await db.products.find_one({"id": product_id, "shop_id": shop_id})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    update_data = {}
    if request.name is not None:
        update_data["name"] = request.name
    if request.price is not None:
        update_data["price"] = request.price
    if request.active is not None:
        update_data["active"] = request.active
    
    await db.products.update_one({"id": product_id}, {"$set": update_data})
    
    updated_product = await db.products.find_one({"id": product_id})
    return Product(**parse_from_mongo(updated_product))

@api_router.delete("/shops/{shop_id}/products/{product_id}")
async def delete_product(shop_id: str, product_id: str, current_user: User = Depends(get_current_user)):
    """Soft delete a product (mark as inactive)"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    await db.products.delete_one(
        {"id": product_id, "shop_id": shop_id}
    )
    
    return {"message": "Product deleted successfully"}

@api_router.get("/shops/{shop_id}/dashboard")
async def get_shop_dashboard(shop_id: str, current_user: User = Depends(get_current_user)):
    """Get shop dashboard data"""
    query = {"id": shop_id}
    if current_user.active_role != "admin":
        query["owner_id"] = current_user.id
        
    shop = await db.shops.find_one(query)
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    total_customers = await db.customers.count_documents({"shop_id": shop_id})
    customers_with_dues = await db.customers.find({"shop_id": shop_id, "balance": {"$lt": 0}}).to_list(length=None)
    total_pending_dues = sum(abs(customer.get("balance", 0)) for customer in customers_with_dues)
    recent_transactions = await db.transactions.find({"shop_id": shop_id}).sort("created_at", -1).limit(5).to_list(length=None)
    total_products = await db.products.count_documents({"shop_id": shop_id, "active": True})
    
    return {
        "shop": Shop(**parse_from_mongo(shop)),
        "total_customers": total_customers,
        "customers_with_dues": len(customers_with_dues),
        "total_pending_dues": total_pending_dues,
        "total_products": total_products,
        "recent_transactions": [Transaction(**parse_from_mongo(t)) for t in recent_transactions]
    }

# ==================== Customer Routes ====================

@api_router.get("/customer/ledger")
async def get_customer_ledger(current_user: User = Depends(get_current_user)):
    """Get customer's ledger across all shops (including staff/services)"""
    # Corrected: Any authenticated user (customer, shop owner, or admin) can view their personal ledger 
    # across shops, even if their active role is temporarily set to something else.
    if current_user.active_role not in ["customer", "shop_owner", "admin"]:
        raise HTTPException(status_code=403, detail="Only authorized users can view ledger")
    
    phone = current_user.phone
    
    # Filter by phone number to find all linked shop records across collections
    customers = await db.customers.find({"phone": phone}).to_list(length=None)
    staff_members = await db.staff.find({"phone": phone}).to_list(length=None)
    services = await db.services.find({"phone": phone}).to_list(length=None)
    
    ledger_data = []
    
    # Process customers
    for customer in customers:
        shop = await db.shops.find_one({"id": customer["shop_id"]})
        if shop:
            transactions = await db.transactions.find({"customer_id": customer["id"]}).sort("created_at", -1).to_list(length=None)
            ledger_data.append({
                "shop": Shop(**parse_from_mongo(shop)),
                "customer": Customer(**parse_from_mongo(customer)),
                "transactions": [Transaction(**parse_from_mongo(t)) for t in transactions],
                "type": "customer"
            })
            
    # Process staff
    for staff in staff_members:
        shop = await db.shops.find_one({"id": staff["shop_id"]})
        if shop:
            ledger_data.append({
                "shop": Shop(**parse_from_mongo(shop)),
                "customer": Staff(**parse_from_mongo(staff)),
                "transactions": [],
                "type": "staff"
            })

    # Process services
    for service in services:
        shop = await db.shops.find_one({"id": service["shop_id"]})
        if shop:
            ledger_data.append({
                "shop": Shop(**parse_from_mongo(shop)),
                "customer": Service(**parse_from_mongo(service)),
                "transactions": [],
                "type": "services"
            })
    
    return ledger_data

# ==================== Public Shop Routes ====================

@api_router.get("/shops/public/{shop_code}")
async def get_shop_by_code(shop_code: str):
    """Get shop details by shop code for public access"""
    shop = await db.shops.find_one({"shop_code": shop_code})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    return {
        "id": shop["id"],
        "name": shop["name"],
        "location": shop["location"],
        "category": shop["category"],
        "shop_code": shop["shop_code"],
        "created_at": shop["created_at"]
    }

@api_router.post("/shops/public/{shop_code}/connect")
async def connect_to_shop_public(shop_code: str, customer_data: dict):
    """Allow customers to connect to shop via QR code/link"""
    shop = await db.shops.find_one({"shop_code": shop_code})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    existing_customer = await db.customers.find_one({
        "shop_id": shop["id"],
        "phone": customer_data["phone"]
    })
    
    if existing_customer:
        raise HTTPException(status_code=409, detail=f"This phone number is already connected as a customer to Shop {shop['name']}")
    
    customer = Customer(
        shop_id=shop["id"],
        name=customer_data["name"],
        phone=customer_data["phone"],
        balance=0,
        is_verified=True
    )
    
    await db.customers.insert_one(prepare_for_mongo(customer.dict()))
    
    return {"message": "Successfully connected to shop", "shop_name": shop["name"], "customer": customer}

# ==================== Public Connect Page ====================

@api_router.get("/public/connect/{shop_code}")
async def view_connect_page(shop_code: str):
    """Public HTML page for customers to connect to a shop via QR code/link"""
    shop = await db.shops.find_one({"shop_code": shop_code})
    if not shop:
        return HTMLResponse(
            content="""
            <html>
                <body style="font-family: sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; background-color: #fce8e8;">
                    <div style="text-align: center; background: white; padding: 40px; border-radius: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                        <h1 style="color: #c53030; margin-bottom: 10px;">Shop Not Found</h1>
                        <p style="color: #666;">This shop link is invalid or the shop no longer exists.</p>
                    </div>
                </body>
            </html>
            """,
            status_code=404
        )
    
    shop_name = shop.get("name", "Shop")
    shop_category = shop.get("category", "")
    
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Connect to Shop SHOP_NAME - XMunim</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                display: flex;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                background: linear-gradient(135deg, #EEF2FF 0%, #F3F4F6 50%, #ECFDF5 100%);
            }
            .container {
                background-color: #FFFFFF;
                border-radius: 24px;
                padding: 40px 24px;
                box-shadow: 0 20px 60px -12px rgba(0, 0, 0, 0.12);
                text-align: center;
                max-width: 420px;
                width: 92%;
            }
            .icon-circle {
                width: 96px;
                height: 96px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto 24px auto;
                transition: all 0.5s ease;
                overflow: hidden;
                background-color: #F3F4F6;
            }
            .icon-circle img {
                width: 100%;
                height: 100%;
                object-fit: contain;
            }
            .icon-shop {
                background: linear-gradient(135deg, #DBEAFE 0%, #93C5FD 100%);
                padding: 10px;
            }
            .icon-emoji {
                font-size: 42px;
            }
            .icon-success {
                background: linear-gradient(135deg, #A7F3D0 0%, #6EE7B7 100%);
                color: #059669;
            }
            .icon-error {
                background: linear-gradient(135deg, #FED7AA 0%, #FDBA74 100%);
                color: #EA580C;
            }
            h1 {
                color: #111827;
                font-size: 24px;
                margin: 0 0 8px 0;
                font-weight: 700;
            }
            .category-badge {
                display: inline-block;
                background: #EEF2FF;
                color: #6366F1;
                font-size: 12px;
                padding: 4px 14px;
                border-radius: 20px;
                margin: 4px 0 16px;
            }
            .desc {
                color: #6B7280;
                font-size: 15px;
                margin-bottom: 24px;
                line-height: 1.5;
            }
            .form-group {
                margin-bottom: 16px;
                text-align: left;
            }
            .form-group label {
                display: block;
                color: #374151;
                font-size: 14px;
                font-weight: 600;
                margin-bottom: 6px;
            }
            .form-group input {
                width: 100%;
                padding: 12px 16px;
                border: 2px solid #E5E7EB;
                border-radius: 12px;
                font-size: 16px;
                outline: none;
                transition: all 0.2s;
                background-color: #F9FAFB;
            }
            .form-group input:focus {
                border-color: #3B82F6;
                background-color: #FFFFFF;
                box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.1);
            }
            .form-group input.invalid {
                border-color: #EF4444;
                background-color: #FEF2F2;
            }
            .error-text {
                color: #EF4444;
                font-size: 12px;
                margin-top: 4px;
                display: none;
                font-weight: 500;
            }
            .form-group input.invalid + .error-text {
                display: block;
            }
            .btn {
                display: block;
                width: 100%;
                padding: 14px;
                border: none;
                border-radius: 12px;
                font-size: 16px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.2s;
                margin-top: 8px;
            }
            .btn-primary {
                background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%);
                color: #fff;
            }
            .btn-primary:hover {
                transform: translateY(-1px);
                box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
            }
            .btn-primary:disabled {
                opacity: 0.6;
                cursor: not-allowed;
                transform: none;
            }
            .success-box {
                background: #ECFDF5;
                border: 1px solid #A7F3D0;
                border-radius: 12px;
                padding: 16px;
                margin-top: 16px;
            }
            .success-box p {
                color: #065F46;
                font-size: 14px;
                line-height: 1.5;
            }
            .error-box {
                background: #FFF7ED;
                border: 1px solid #FDBA74;
                border-radius: 12px;
                padding: 16px;
                margin-top: 16px;
            }
            .error-box p {
                color: #9A3412;
                font-size: 14px;
                line-height: 1.5;
            }
            .branding {
                margin-top: 24px;
                color: #D1D5DB;
                font-size: 12px;
            }
            .branding strong {
                color: #9CA3AF;
            }
            .hidden { display: none; }
            .spinner {
                display: inline-block;
                width: 18px;
                height: 18px;
                border: 3px solid rgba(255,255,255,0.3);
                border-radius: 50%;
                border-top-color: #fff;
                animation: spin 0.8s linear infinite;
                vertical-align: middle;
                margin-right: 8px;
            }
            @keyframes spin {
                to { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <!-- Initial Form State -->
            <div id="formState">
                <div class="icon-circle icon-shop">
                    <img src="/static/icon-v3.png" alt="XMunim" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                    <span class="icon-emoji" style="display:none;">🏪</span>
                </div>
                <h1>Shop SHOP_NAME</h1>
                CATEGORY_BADGE
                <p class="desc">Join this shop on XMunim to view items, make requests, and track payments.</p>
                
                <form id="connectForm" onsubmit="handleConnect(event)" novalidate>
                    <div class="form-group">
                        <label for="name">Your Name</label>
                        <input type="text" id="name" placeholder="Enter your full name" required oninput="validateField(this)" />
                        <span class="error-text">Please enter your name</span>
                    </div>
                    <div class="form-group">
                        <label for="phone">Phone Number</label>
                        <input type="tel" id="phone" placeholder="10-digit phone number" 
                            pattern="[0-9]{10}" maxlength="10" required 
                            oninput="this.value = this.value.replace(/[^0-9]/g, ''); validateField(this)" />
                        <span class="error-text">Please enter a valid 10-digit number</span>
                    </div>
                    <button type="submit" class="btn btn-primary" id="submitBtn">Connect to Shop</button>
                </form>
            </div>

            <!-- Success State -->
            <div id="successState" class="hidden">
                <div class="icon-circle icon-success">
                    <span class="icon-emoji">✅</span>
                </div>
                <h1>Connected!</h1>
                <div class="success-box">
                    <p>You are now connected to <strong>Shop SHOP_NAME</strong> as a verified customer.</p>
                    <p style="margin-top:8px;">The shop owner can now see you in their customer list.</p>
                </div>
                <p class="branding">Powered by <strong>XMunim</strong></p>
            </div>

            <!-- Error State -->
            <div id="errorState" class="hidden">
                <div class="icon-circle icon-error">
                    <span class="icon-emoji">⚠️</span>
                </div>
                <h1>Already Connected</h1>
                <div class="error-box">
                    <p id="errorMessage">This phone number is already connected as a customer.</p>
                </div>
                <button class="btn btn-primary" style="margin-top:16px;" onclick="resetForm()">Try Another Number</button>
                <p class="branding">Powered by <strong>XMunim</strong></p>
            </div>
        </div>

        <script>
            function validateField(input) {
                if (input.id === 'phone') {
                    if (input.value.length === 10 && /^[0-9]+$/.test(input.value)) {
                        input.classList.remove('invalid');
                    } else if (input.value.length > 0) {
                        input.classList.add('invalid');
                    }
                } else {
                    if (input.value.trim().length > 0) {
                        input.classList.remove('invalid');
                    }
                }
            }

            async function handleConnect(e) {
                e.preventDefault();
                var nameInput = document.getElementById('name');
                var phoneInput = document.getElementById('phone');
                var name = nameInput.value.trim();
                var phone = phoneInput.value.trim();
                var btn = document.getElementById('submitBtn');
                
                var isValid = true;
                if (!name) {
                    nameInput.classList.add('invalid');
                    isValid = false;
                }
                if (phone.length !== 10) {
                    phoneInput.classList.add('invalid');
                    isValid = false;
                }

                if (!isValid) return;
                
                btn.disabled = true;
                btn.innerHTML = '<span class="spinner"></span> Connecting...';
                
                try {
                    var response = await fetch('/api/shops/public/SHOP_CODE/connect', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ name: name, phone: phone })
                    });
                    
                    if (response.ok) {                 // Success: Hide form, show success
                        document.getElementById('formState').classList.add('hidden');
                        document.getElementById('successState').classList.remove('hidden');
                        
                        // Update success message based on type
                        const successText = document.querySelector('#successState .success-box p:first-child');
                        const shopName = document.querySelector('#formState h1').textContent.replace('Shop ', '');
                        const isService = window.location.pathname.includes('service');
                        successText.innerHTML = `You are now connected to <strong>${shopName}</strong> as a verified ${isService ? 'member' : 'customer'}.`;
                    } else if (response.status === 409) {
                        var data = await response.json();
                        document.getElementById('errorMessage').textContent = data.detail || 'This phone number is already connected as a customer.';
                        document.getElementById('formState').classList.add('hidden');
                        document.getElementById('errorState').classList.remove('hidden');
                    } else {
                        var errData = await response.json();
                        alert(errData.detail || 'Something went wrong. Please try again.');
                        btn.disabled = false;
                        btn.textContent = 'Connect to Shop';
                    }
                } catch (err) {
                    alert('Network error. Please check your connection and try again.');
                    btn.disabled = false;
                    btn.textContent = 'Connect to Shop';
                }
            }
            
            function resetForm() {
                document.getElementById('errorState').classList.add('hidden');
                document.getElementById('formState').classList.remove('hidden');
                document.getElementById('phone').value = '';
                document.getElementById('submitBtn').disabled = false;
                document.getElementById('submitBtn').textContent = 'Connect to Shop';
            }
        </script>
    </body>
    </html>
    """
    
    category_html = f'<span class="category-badge">{shop_category}</span>' if shop_category else ''
    
    html_content = html_template.replace("SHOP_NAME", shop_name).replace("SHOP_CODE", shop_code).replace("CATEGORY_BADGE", category_html)
    
    return HTMLResponse(content=html_content)

# ==================== Admin Routes ====================

@api_router.get("/admin/dashboard")
async def get_admin_dashboard(admin_user: User = Depends(get_admin_user)):
    """Get admin dashboard with key metrics"""
    total_users = await db.users.count_documents({})
    total_shops = await db.shops.count_documents({})
    total_customers = await db.customers.count_documents({})
    
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30))
    recent_shop_transactions = await db.transactions.aggregate([
        {"$match": {"created_at": {"$gte": thirty_days_ago}}},
        {"$group": {"_id": "$shop_id"}}
    ]).to_list(length=None)
    active_shops = len(recent_shop_transactions)
    
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_transactions = await db.transactions.find({"created_at": {"$gte": today_start}}).to_list(length=None)
    daily_transactions_count = len(today_transactions)
    daily_transactions_amount = sum(t.get("amount", 0) for t in today_transactions)
    
    all_transactions = await db.transactions.find({}).to_list(length=None)
    total_amount = sum(t.get("amount", 0) for t in all_transactions)
    # Calculate total sales (only credit transactions)
    total_sales = sum(t.get("amount", 0) for t in all_transactions if str(t.get("type", "")).lower() == "credit")
    
    seven_days_ago = (datetime.now(timezone.utc) - timedelta(days=7))
    new_users_count = await db.users.count_documents({"created_at": {"$gte": seven_days_ago}})
    
    return {
        "total_users": total_users,
        "total_shops": total_shops,
        "active_shops": active_shops,
        "total_customers": total_customers,
        "daily_transactions": {
            "count": daily_transactions_count,
            "amount": daily_transactions_amount
        },
        "total_amount": total_amount,
        "total_sales": total_sales,
        "new_users_this_week": new_users_count
    }

@api_router.get("/admin/users")
async def get_all_users(admin_user: User = Depends(get_admin_user), search: Optional[str] = None, skip: int = 0, limit: int = 100):
    """Get all users with optional search"""
    query = {}
    if search:
        query = {
            "$or": [
                {"phone": {"$regex": search, "$options": "i"}},
                {"name": {"$regex": search, "$options": "i"}}
            ]
        }
    
    users = await db.users.find(query).sort("created_at", -1).skip(skip).limit(limit).to_list(length=None)
    total_count = await db.users.count_documents(query)
    
    return {
        "users": [User(**parse_from_mongo(user)) for user in users],
        "total": total_count,
        "skip": skip,
        "limit": limit
    }

@api_router.get("/admin/shops")
async def get_all_shops(admin_user: User = Depends(get_admin_user), search: Optional[str] = None, skip: int = 0, limit: int = 100):
    """Get all shops with optional search"""
    query = {}
    if search:
        query = {
            "$or": [
                {"name": {"$regex": search, "$options": "i"}},
                {"location": {"$regex": search, "$options": "i"}},
                {"shop_code": {"$regex": search, "$options": "i"}}
            ]
        }
    
    shops = await db.shops.find(query).sort("created_at", -1).skip(skip).limit(limit).to_list(length=None)
    total_count = await db.shops.count_documents(query)
    
    shops_with_owners = []
    for shop in shops:
        owner = await db.users.find_one({"id": shop["owner_id"]})
        shop_data = Shop(**parse_from_mongo(shop)).dict()
        shop_data["owner"] = User(**parse_from_mongo(owner)).dict() if owner else None
        shops_with_owners.append(shop_data)
    
    return {
        "shops": shops_with_owners,
        "total": total_count,
        "skip": skip,
        "limit": limit
    }

@api_router.put("/admin/users/{user_id}")
async def update_user_status(user_id: str, request: UserVerifyRequest, admin_user: User = Depends(get_admin_user)):
    """Update user verification and flag status"""
    update_data = {}
    if request.verified is not None:
        update_data["verified"] = request.verified
    if request.flagged is not None:
        update_data["flagged"] = request.flagged
    
    if not update_data:
        raise HTTPException(status_code=400, detail="No update data provided")
    
    result = await db.users.update_one({"id": user_id}, {"$set": update_data})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    updated_user = await db.users.find_one({"id": user_id})
    return {"user": User(**parse_from_mongo(updated_user)), "message": "User status updated successfully"}

@api_router.get("/admin/transactions")
async def get_all_transactions(admin_user: User = Depends(get_admin_user), skip: int = 0, limit: int = 100):
    """Get all transactions"""
    transactions = await db.transactions.find({}).sort("created_at", -1).skip(skip).limit(limit).to_list(length=None)
    total_count = await db.transactions.count_documents({})
    
    transactions_with_details = []
    for transaction in transactions:
        shop = await db.shops.find_one({"id": transaction["shop_id"]})
        customer = await db.customers.find_one({"id": transaction["customer_id"]})
        
        transaction_data = Transaction(**parse_from_mongo(transaction)).dict()
        transaction_data["shop"] = Shop(**parse_from_mongo(shop)).dict() if shop else None
        transaction_data["customer"] = Customer(**parse_from_mongo(customer)).dict() if customer else None
        transactions_with_details.append(transaction_data)
    
    return {
        "transactions": transactions_with_details,
        "total": total_count,
        "skip": skip,
        "limit": limit
    }

@api_router.get("/admin/customers")
async def get_all_customers(admin_user: User = Depends(get_admin_user), search: Optional[str] = None, skip: int = 0, limit: int = 100):
    """Get all customers across all shops"""
    query = {}
    if search:
        query = {
            "$or": [
                {"name": {"$regex": search, "$options": "i"}},
                {"phone": {"$regex": search, "$options": "i"}}
            ]
        }
    
    customers = await db.customers.find(query).sort("created_at", -1).skip(skip).limit(limit).to_list(length=None)
    total_count = await db.customers.count_documents(query)
    
    customers_with_details = []
    for customer in customers:
        shop = await db.shops.find_one({"id": customer["shop_id"]})
        customer_data = Customer(**parse_from_mongo(customer)).dict()
        if shop:
            customer_data["shop"] = Shop(**parse_from_mongo(shop)).dict()
            
        # Enrich with transaction stats
        # For performance in a real app, this should be an aggregation or stored on customer document
        # But for this size, we can query.
        txs = await db.transactions.find({"customer_id": customer["id"], "shop_id": customer["shop_id"]}).sort("date", -1).to_list(length=None)
        customer_data["total_transactions"] = len(txs)
        # txs[0]["date"] is already an ISO string in MongoDB, so we use it directly.
        # If it were a datetime object (from parse_from_mongo), we would need isoformat.
        # But db.find returns raw dicts.
        customer_data["last_transaction_date"] = txs[0]["date"] if txs else None
            
        customers_with_details.append(customer_data)
    
    total_tx_count = await db.transactions.count_documents({})

    return {
        "customers": customers_with_details,
        "total": total_count,
        "total_global_transactions": total_tx_count,
        "skip": skip,
        "limit": limit
    }

@api_router.get("/shops/{shop_id}/notifications")
async def get_shop_notifications(shop_id: str, current_user: User = Depends(get_current_user)):
    """Get all payment requests/notifications sent by a specific shop"""
    # 1. Verify shop ownership
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found or access denied")

    # 2. Get all payment requests for this shop
    notifications = await db.payment_requests.find(
        {"shop_id": shop_id},
        sort=[("created_at", -1)]
    ).to_list(length=100) # Limit to last 100 for now

    # 3. Enrich with customer info
    enriched_notifications = []
    for noti in notifications:
        customer = await db.customers.find_one({"id": noti["customer_id"]})
        noti_data = parse_from_mongo(noti)
        noti_data["customer_name"] = customer["name"] if customer else "Unknown Customer"
        # Handle cases where existing logs don't have a title
        if "title" not in noti_data:
            noti_data["title"] = "Payment Request"
        enriched_notifications.append(noti_data)

    return enriched_notifications

@api_router.get("/shops/{shop_id}/customers/{customer_id}/notifications")
async def get_customer_payment_history(shop_id: str, customer_id: str, current_user: User = Depends(get_current_user)):
    """Get payment request history for a specific customer in a shop"""
    # 1. Verify shop ownership
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found or access denied")

    # 2. Get all payment requests for this specific customer
    notifications = await db.payment_requests.find(
        {"shop_id": shop_id, "customer_id": customer_id},
        sort=[("created_at", -1)]
    ).to_list(length=None)

    # 3. Parse and return
    return [parse_from_mongo(noti) for noti in notifications]

@api_router.get("/customer/notifications")
async def get_customer_notifications(current_user: User = Depends(get_current_user)):
    """Get all payment requests/notifications for the current customer (by phone)"""
    # 1. Find all customer entries for this phone number across all shops
    customers = await db.customers.find({"phone": current_user.phone}).to_list(length=None)
    customer_ids = [c["id"] for c in customers]
    
    if not customer_ids:
        return []

    # 2. Get all payment requests for these customer IDs
    notifications = await db.payment_requests.find(
        {"customer_id": {"$in": customer_ids}},
        sort=[("created_at", -1)]
    ).to_list(length=None)

    # 3. Enrich with shop info
    enriched_notifications = []
    for noti in notifications:
        shop = await db.shops.find_one({"id": noti["shop_id"]})
        noti_data = parse_from_mongo(noti)
        noti_data["shop_name"] = shop["name"] if shop else "Unknown Shop"
        # Handle cases where existing logs don't have a title
        if "title" not in noti_data:
            noti_data["title"] = "Payment Request"
        enriched_notifications.append(noti_data)

    return enriched_notifications

# ==================== EXTERNAL PROXY PINCODE====================

@api_router.get("/location/pincode/{pincode}")
async def get_pincode_details(pincode: str):
    """
    Proxy endpoint to fetch location details for a pincode from postalpincode.in.
    This bypasses any client-side interceptors (e.g. MSG91 widget) that block standard frontend fetch calls.
    """
    if len(pincode) != 6 or not pincode.isdigit():
        raise HTTPException(status_code=400, detail="Invalid pincode format")
        
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json"
    }
    
    try:
        # Try primary API first (often blocks/fails)
        url = f"https://api.postalpincode.in/pincode/{pincode}"
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as primary_error:
        logger.warning(f"Primary Pincode API failed for {pincode}: {primary_error}. Trying reliable global API...")
        
        try:
            # Most reliable fallback (Zippopotam.us - India)
            fallback_url = f"http://api.zippopotam.us/in/{pincode}"
            fallback_response = requests.get(fallback_url, headers=headers, timeout=10)
            fallback_response.raise_for_status()
            
            data = fallback_response.json()
            
            # Translate Zippopotam format to match Postalpincode.in format so frontend needs no changes
            formatted_data = [{
                "Message": "Number of pincode(s) found",
                "Status": "Success",
                "PostOffice": []
            }]
            
            for place in data.get("places", []):
                formatted_data[0]["PostOffice"].append({
                    "Name": place.get("place name", ""),
                    "District": place.get("state abbreviation", data.get("country", "")), # Zippopotam lacks district often
                    "State": place.get("state", ""),
                    "Country": data.get("country", "India"),
                    "Pincode": pincode
                })
            
            return formatted_data
        except Exception as fallback_error:
            logger.error(f"All Pincode APIs failed for {pincode}: {fallback_error}")
            raise HTTPException(status_code=503, detail="Location service temporarily unavailable")

# ==================== App Configuration ====================

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logger is already configured at the top

@app.on_event("startup")
async def startup_event():
    # Create TTL index on payment_requests (notifications)
    # 2592000 seconds = 30 days
    await db.payment_requests.create_index(
        "created_at",
        expireAfterSeconds=2592000,
        name="ttl_index_30_days"
    )
    logger.info("Checked/Created TTL index on payment_requests.created_at for 30-day auto-delete")
    
    asyncio.create_task(reminder_worker())
    logger.info("Server startup complete. Background worker started.")

async def reminder_worker():
    """Background worker for automated reminders"""
    while True:
        try:
            logger.info("Running automated reminder scan...")
            # 1. Process One-off Scheduled Reminders (All customers)
            now = datetime.now(timezone.utc)
            now_str = now.isoformat()
            
            scheduled_items = await db.payment_requests.find({
                "status": "pending",
                "scheduled_at": {"$ne": None},
                "$or": [
                    {"scheduled_at": {"$lte": now}},
                    {"scheduled_at": {"$lte": now_str}}
                ]
            }).to_list(length=None)

            if scheduled_items:
                logger.info(f"Found {len(scheduled_items)} pending scheduled reminders to process.")

            for item in scheduled_items:
                try:
                    customer = await db.customers.find_one({"id": item["customer_id"]})
                    if not customer:
                        await db.payment_requests.update_one({"id": item["id"]}, {"$set": {"status": "failed", "error": "Customer not found"}})
                        continue

                    if item["method"] == "Push Notification":
                        user = await db.users.find_one({"phone": customer["phone"]})
                        if user and user.get("fcm_token"):
                            logger.info(f"Worker Sending Push: Title='{item['title']}', Body='{item['message']}' to {customer['name']}")
                            message = messaging.Message(
                                notification=messaging.Notification(title=item["title"], body=item["message"]),
                                android=messaging.AndroidConfig(
                                    notification=messaging.AndroidNotification(
                                        color="#304FFE",
                                        channel_id="default"
                                    )
                                ),
                                data={"customerId": customer["id"]},
                                token=user["fcm_token"],
                            )
                            messaging.send(message)
                            await db.payment_requests.update_one({"id": item["id"]}, {"$set": {"status": "sent"}})
                            logger.info(f"Scheduled reminder sent to {customer['name']}")
                        else:
                            await db.payment_requests.update_one({"id": item["id"]}, {"$set": {"status": "failed", "error": "FCM token missing"}})
                    else:
                        # Mark SMS/WhatsApp as sent since they are logged for history
                        await db.payment_requests.update_one({"id": item["id"]}, {"$set": {"status": "sent"}})
                except Exception as e:
                    logger.error(f"Failed to send scheduled reminder {item.get('id')}: {e}")
                    await db.payment_requests.update_one({"id": item.get("id")}, {"$set": {"status": "failed"}})

            # 2. Find all customers with auto-reminders enabled and balance < 0
            customers = await db.customers.find({
                "is_auto_reminder_enabled": True,
                "balance": {"$lt": 0}
            }).to_list(length=None)

            for customer in customers:
                # 3. Check last transaction date and last reminder date
                shop = await db.shops.find_one({"id": customer["shop_id"]})
                if not shop:
                    continue

                # Get last reminder for this customer
                last_reminder = await db.payment_requests.find_one(
                    {"customer_id": customer["id"], "status": "sent"},
                    sort=[("created_at", -1)]
                )

                # Dynamic logic for delay mapping
                delay_str = customer.get("auto_reminder_delay", "3 days overdue")
                delay_days = 3
                if "1 day" in delay_str:
                    delay_days = 1
                elif "7 days" in delay_str:
                    delay_days = 7
                elif "15 days" in delay_str:
                    delay_days = 15
                elif "30 days" in delay_str:
                    delay_days = 30

                should_send = False
                now = datetime.now(timezone.utc)
                
                if not last_reminder:
                    # Check if enough days have passed since the last transaction to trigger the FIRST reminder
                    last_txn_str = customer.get("last_transaction_date")
                    if last_txn_str:
                        # Convert ISO format date to offset-aware UTC datetime
                        try:
                            last_txn_date = datetime.fromisoformat(last_txn_str.replace("Z", "+00:00"))
                        except ValueError:
                            last_txn_date = now - timedelta(days=delay_days) # fallback trigger immediately if invalid date
                        
                        if (now - last_txn_date).days >= delay_days:
                            should_send = True
                    else:
                        # If no last transaction date is recorded, trigger it based on creation or immediately.
                        should_send = True
                else:
                    # Check frequency if already sent once
                    last_sent = parse_from_mongo(last_reminder)["created_at"]
                    freq = customer.get("auto_reminder_frequency", "Daily until paid")
                    
                    if freq == "Send once only":
                        should_send = False
                    elif freq == "Daily until paid" and (now - last_sent).days >= 1:
                        should_send = True
                    elif freq == "Weekly until paid" and (now - last_sent).days >= 7:
                        should_send = True
                    elif freq == "Every 2 weeks" and (now - last_sent).days >= 14:
                        should_send = True

                if should_send and customer.get("auto_reminder_method") == "Push Notification":
                    user = await db.users.find_one({"phone": customer["phone"]})
                    if user and user.get("fcm_token") and user.get("push_enabled", True) and user.get("payment_alerts_enabled", True):
                        title = "Payment Reminder"
                        
                        # Use custom message if available, otherwise use default
                        custom_msg = customer.get("auto_reminder_message")
                        if custom_msg and custom_msg.strip():
                            body = custom_msg.replace("{name}", customer["name"])\
                                             .replace("{amount}", f"{abs(customer['balance']):.2f}")\
                                             .replace("{delay}", customer.get("auto_reminder_delay", "3 days overdue"))\
                                             .replace("{frequency}", customer.get("auto_reminder_frequency", "Daily until paid"))
                        else:
                            body = f"Hello {customer['name']}, you have a pending payment of ₹{abs(customer['balance']):.2f} at {shop['name']}. Please settle your dues. Thank you!"
                        
                        try:
                            message = messaging.Message(
                                notification=messaging.Notification(title=title, body=body),
                                data={"customerId": customer["id"]},
                                token=user["fcm_token"],
                            )
                            messaging.send(message)
                            
                            # Log the auto-reminder
                            payment_req = PaymentRequest(
                                shop_id=customer["shop_id"],
                                customer_id=customer["id"],
                                amount=abs(customer["balance"]),
                                method="Push Notification",
                                title=title,
                                message=body,
                                status="sent"
                            )
                            await db.payment_requests.insert_one(prepare_for_mongo(payment_req.dict()))
                            logger.info(f"Auto-reminder sent to {customer['name']}")
                        except Exception as e:
                            error_str = str(e)
                            if "SenderId mismatch" in error_str or "Unregistered" in error_str or "Requested entity was not found" in error_str:
                                logger.warning(f"Invalid FCM token for user {user['phone']} ({error_str}). Removing token.")
                                await db.users.update_one({"id": user["id"]}, {"$set": {"fcm_token": None}})
                            else:
                                logger.error(f"Failed to send auto-reminder: {e}")

        except Exception as e:
            logger.error(f"Error in reminder worker: {e}")
        
        # Sleep for 60 seconds between scans (for testing responsiveness)
        await asyncio.sleep(60) 

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

# Health check endpoint
@app.get("/")
async def root():
    return {"status": "ok", "message": "XMunim App Backend is running"}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# ==================== Android App Links ====================

@app.get("/.well-known/assetlinks.json")
async def get_assetlinks():
    """
    Serve the Digital Asset Links file for Android App Links.
    This allows the app to verify ownership of the domain and open links directly.
    """
    return [
        {
            "relation": ["delegate_permission/common.handle_all_urls"],
            "target": {
                "namespace": "android_app",
                "package_name": "com.krutik3011.xmunimapp",
                "sha256_cert_fingerprints": [
                    # IMPORTANT: This must be replaced with your production SHA-256 fingerprint
                    "7C:8A:9D:14:B0:CA:9A:AB:A2:09:BF:B2:26:C0:9F:1A:43:3A:26:FA:6C:01:CE:F0:B0:26:04:54:9E:2B:5E:36"]
            }
        }
    ]

