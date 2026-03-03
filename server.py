from fastapi import FastAPI, APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
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

# Configure logging right after imports
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
db = client[os.environ.get('DB_NAME', 'shopmunim_app')]

# Create the main app without a prefix
app = FastAPI(title="ShopMunim App Backend", version="1.0.0")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# JWT Configuration
JWT_SECRET = "shopmunim_secret_key_2024"
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
    balance: float = 0.0  # negative means customer owes money
    is_auto_reminder_enabled: bool = False
    auto_reminder_delay: str = "3 days overdue"
    auto_reminder_frequency: str = "Daily until paid"
    auto_reminder_method: str = "Push Notification"
    auto_reminder_message: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Product(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shop_id: str
    name: str
    price: float
    active: bool = True
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
    is_auto_reminder_enabled: Optional[bool] = False
    auto_reminder_delay: Optional[str] = "3 days overdue"
    auto_reminder_frequency: Optional[str] = "Daily until paid"
    auto_reminder_method: Optional[str] = "Push Notification"
    auto_reminder_message: Optional[str] = None

class ProductCreateRequest(BaseModel):
    name: str
    price: float

class ProductUpdateRequest(BaseModel):
    name: Optional[str] = None
    price: Optional[float] = None
    active: Optional[bool] = None

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
    is_auto_reminder_enabled: Optional[bool] = None
    auto_reminder_delay: Optional[str] = None
    auto_reminder_frequency: Optional[str] = None
    auto_reminder_method: Optional[str] = None
    auto_reminder_message: Optional[str] = None

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
    """Parse datetime strings or objects back from MongoDB and remove _id"""
    if item is None:
        return None
    if isinstance(item, dict):
        item = item.copy()  # Don't modify original
        if '_id' in item:
            item.pop('_id')
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
    """Send OTP to phone number (mocked for MVP)"""
    if request.is_login:
        # Search for user with same phone number
        user_exists = await db.users.find_one({"phone": request.phone})
        if not user_exists:
            raise HTTPException(status_code=404, detail="User does not exist")
    else:
        # Check if user with same phone already exists
        if not request.name:
            raise HTTPException(status_code=400, detail="Name is required for sign up")
            
        user_exists = await db.users.find_one({"phone": request.phone})
        if user_exists:
            raise HTTPException(status_code=400, detail="Phone number already registered.")

    mock_otp = "123456"
    
    await db.otps.delete_many({"phone": request.phone})
    await db.otps.insert_one({
        "phone": request.phone,
        "otp": mock_otp,
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    return {"message": "OTP sent successfully", "mock_otp": mock_otp}

@api_router.post("/auth/verify-otp")
async def verify_otp(request: OTPVerifyRequest):
    """Verify OTP and login user"""
    otp_record = await db.otps.find_one({"phone": request.phone, "otp": request.otp})
    if not otp_record:
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
    
    await db.otps.delete_many({"phone": request.phone})
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
    
    Story.append(Paragraph("ShopMunim - Data Export Report", styles['Title']))
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

@api_router.delete("/auth/me")
async def delete_account(current_user: User = Depends(get_current_user)):
    """Permanently delete user account and associated customer records"""
    # 1. Delete all customer records associated with this phone number
    await db.customers.delete_many({"phone": current_user.phone})
    
    # 2. If user is a shop owner, we might want to handle their shops
    # For now, we'll just remove the user record to keep it simple
    # In a production app, you'd handle shop ownership transfer or deletion
    
    # 3. Delete the user record
    result = await db.users.delete_one({"id": current_user.id})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
        
    return {"message": "Account and associated data deleted successfully"}

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
    print(f"[DEBUG] Shop: {shop_id}, Found {len(customers)} customers")
    
    # All-time stats for the summary cards
    total_customers_count = len(customers)
    all_time_with_dues_count = 0
    all_time_total_dues = 0
    for c in customers:
        bal = c.get("balance", 0)
        if bal < 0:
            all_time_with_dues_count += 1
            all_time_total_dues += abs(bal)
    
    print(f"[DEBUG] All-time Dues: {all_time_with_dues_count} customers, Total ₹{all_time_total_dues}")

    # Date Filter setup
    tx_query = {"shop_id": shop_id}
    date_filter = None
    if from_date or to_date:
        date_filter = {}
        if from_date:
            date_filter["$gte"] = from_date
        if to_date:
            # If to_date is already an ISO string (contains 'T'), use it as is
            # Otherwise, append T23:59:59.999 for YYYY-MM-DD format
            if 'T' in to_date:
                date_filter["$lte"] = to_date
            else:
                date_filter["$lte"] = to_date + "T23:59:59.999"
        tx_query["date"] = date_filter
    
    # Period-specific stats
    all_transactions = await db.transactions.find(tx_query).to_list(length=None)
    print(f"[DEBUG] tx_query: {tx_query}, Found {len(all_transactions)} transactions")
    
    # Use float() to ensure numerical sums even if stored as strings (though they should be floats)
    def safe_amt(tx):
        try:
            return float(tx.get("amount", 0))
        except (TypeError, ValueError):
            return 0.0

    period_amount = sum(safe_amt(tx) for tx in all_transactions)
    # Handle "credit"/sales
    period_sales = sum(safe_amt(tx) for tx in all_transactions if str(tx.get("type", "")).lower() == "credit")
    # Handle "payment"/"debit"
    period_payments = sum(safe_amt(tx) for tx in all_transactions if str(tx.get("type", "")).lower() in ["payment", "debit"])
    
    # Identify active customers and calculate period deltas per customer
    customer_period_deltas = {}
    active_customer_ids = set()
    for tx in all_transactions:
        cid = str(tx.get("customer_id", ""))
        if cid:
            active_customer_ids.add(cid)
            amt = safe_amt(tx)
            tx_type = str(tx.get("type", "")).lower()
            
            if tx_type == "credit":
                customer_period_deltas[cid] = customer_period_deltas.get(cid, 0) - amt
            elif tx_type in ["payment", "debit"]:
                customer_period_deltas[cid] = customer_period_deltas.get(cid, 0) + amt

    # Enrichment loop for list cards
    customers_with_details = []
    for customer in customers:
        c_id = str(customer.get("id", ""))
        # If date filter active, skip customers with no transactions in the range
        if (from_date or to_date) and c_id not in active_customer_ids:
            continue

        customer_data = Customer(**parse_from_mongo(customer)).dict()
        
        # Add period specific delta
        customer_data["period_delta"] = customer_period_deltas.get(c_id, 0)
        
        # Enrich with transaction stats (RESPECT DATE FILTER ON THE CARD TOO)
        tx_query_card = {"customer_id": c_id, "shop_id": shop_id}
        if date_filter:
            tx_query_card["date"] = date_filter
            
        txs_card = await db.transactions.find(tx_query_card).sort("date", -1).to_list(length=None)
        customer_data["total_transactions"] = len(txs_card)
        customer_data["last_transaction_date"] = txs_card[0]["date"] if txs_card else None
        
        customers_with_details.append(customer_data)
    
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
        "total_amount": period_amount,
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
        raise HTTPException(status_code=404, detail="Shop not found")
    
    # Check if the customer already exists in this specific shop
    existing_customer = await db.customers.find_one({"shop_id": shop_id, "phone": request.phone})
    if existing_customer:
        raise HTTPException(status_code=400, detail="Customer already exists")
    
    customer = Customer(
        shop_id=shop_id,
        name=request.name,
        phone=request.phone,
        nickname=request.nickname
    )
    
    
    customer_dict = prepare_for_mongo(customer.dict())
    await db.customers.insert_one(customer_dict)
    return customer

@api_router.put("/shops/{shop_id}/customers/{customer_id}", response_model=Customer)
async def update_customer(shop_id: str, customer_id: str, request: CustomerUpdateRequest, current_user: User = Depends(get_current_user)):
    """Update customer details"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")

    customer = await db.customers.find_one({"id": customer_id, "shop_id": shop_id})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    update_data = {}
    if request.name is not None:
        update_data["name"] = request.name
    if request.phone is not None:
        update_data["phone"] = request.phone
    if request.nickname is not None:
        update_data["nickname"] = request.nickname
    if request.is_auto_reminder_enabled is not None:
        update_data["is_auto_reminder_enabled"] = request.is_auto_reminder_enabled
    if request.auto_reminder_delay is not None:
        update_data["auto_reminder_delay"] = request.auto_reminder_delay
    if request.auto_reminder_frequency is not None:
        update_data["auto_reminder_frequency"] = request.auto_reminder_frequency
    if request.auto_reminder_method is not None:
        update_data["auto_reminder_method"] = request.auto_reminder_method

    if not update_data:
        raise HTTPException(status_code=400, detail="No update data provided")

    await db.customers.update_one(
        {"id": customer_id, "shop_id": shop_id},
        {"$set": update_data}
    )

    updated_customer = await db.customers.find_one({"id": customer_id})
    return Customer(**parse_from_mongo(updated_customer))

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

@api_router.post("/shops/{shop_id}/transactions", response_model=Transaction)
async def create_transaction(shop_id: str, request: TransactionCreateRequest, current_user: User = Depends(get_current_user)):
    """Create a new transaction"""
    shop = await db.shops.find_one({"id": shop_id, "owner_id": current_user.id})
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found")
    
    customer = await db.customers.find_one({"id": request.customer_id, "shop_id": shop_id})
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    
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
    """Get customer's ledger across all shops"""
    if current_user.active_role != "customer":
        raise HTTPException(status_code=403, detail="Only customers can view ledger")
    
    # Filter customers by phone number to find all linked shop records
    customers = await db.customers.find({
        "phone": current_user.phone
    }).to_list(length=None)
    
    ledger_data = []
    for customer in customers:
        shop = await db.shops.find_one({"id": customer["shop_id"]})
        if shop:
            transactions = await db.transactions.find({"customer_id": customer["id"]}).sort("created_at", -1).to_list(length=None)
            
            ledger_data.append({
                "shop": Shop(**parse_from_mongo(shop)),
                "customer": Customer(**parse_from_mongo(customer)),
                "transactions": [Transaction(**parse_from_mongo(t)) for t in transactions]
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
        raise HTTPException(status_code=409, detail="Customer already connected to this shop")
    
    customer = Customer(
        shop_id=shop["id"],
        name=customer_data["name"],
        phone=customer_data["phone"],
        balance=0
    )
    
    await db.customers.insert_one(prepare_for_mongo(customer.dict()))
    
    return {"message": "Successfully connected to shop", "customer": customer}

# ==================== Admin Routes ====================

@api_router.get("/admin/dashboard")
async def get_admin_dashboard(admin_user: User = Depends(get_admin_user)):
    """Get admin dashboard with key metrics"""
    total_users = await db.users.count_documents({})
    total_shops = await db.shops.count_documents({})
    total_customers = await db.customers.count_documents({})
    
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    recent_shop_transactions = await db.transactions.aggregate([
        {"$match": {"created_at": {"$gte": thirty_days_ago}}},
        {"$group": {"_id": "$shop_id"}}
    ]).to_list(length=None)
    active_shops = len(recent_shop_transactions)
    
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    today_transactions = await db.transactions.find({"created_at": {"$gte": today_start}}).to_list(length=None)
    daily_transactions_count = len(today_transactions)
    daily_transactions_amount = sum(t.get("amount", 0) for t in today_transactions)
    
    all_transactions = await db.transactions.find({}).to_list(length=None)
    total_amount = sum(t.get("amount", 0) for t in all_transactions)
    # Calculate total sales (only credit transactions)
    total_sales = sum(t.get("amount", 0) for t in all_transactions if str(t.get("type", "")).lower() == "credit")
    
    seven_days_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    return {"status": "ok", "message": "ShopMunim App Backend is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
