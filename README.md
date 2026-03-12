# 🏪 XMunim Backend

A robust, asynchronous API server for XMunimApp built with **FastAPI** and **MongoDB**. This backend serves the Frontend for Customers, Shop Owners, and Admins.

---

## 🛠️ Tech Stack

- **FastAPI**: Modern, high-performance web framework for building APIs with Python 3.9+.
- **MongoDB (Motor)**: Asynchronous driver for MongoDB.
- **Pydantic**: Data validation and settings management using Python type annotations.
- **JWT (Jose)**: Secure token-based authentication.
- **Uvicorn**: Lightning-fast ASGI server.
- **Dotenv**: Manage environment variables.
- **MSG91**: SMS Gateway for OTP authentication.
- **Firebase Admin SDK**: Push notifications and messaging.
- **ReportLab**: Server-side PDF generation.
- **Axios (Node.js)**: Used for internal scripting/integrations.

---

## 🏗️ Project Structure

```text
XMunim-Backend/
├── server.py              # Main application entry point & API Router
├── requirements.txt       # Python backend dependencies
├── package.json           # Node.js dependencies (Axios for legacy/scripting)
├── .gitignore             # Git exclusion rules (Python/Node/OS)
├── .env                   # Environment config (Private)
├── static/                # Publicly accessible assets (Uploaded photos)
│   └── profile_photos/    # User-uploaded profile images
├── venv/                  # Python virtual environment (Local)
```

---

## 📦 Prerequisites

- **Python** (v3.9+) - [Download](https://python.org/)
- **Node.js** (v18+) - [Download](https://nodejs.org/)
- **MongoDB** - [Download](https://mongodb.com/try/download/community)

---

## 🚀 Installation

```bash
# Clone the repository
cd XMunim-Backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Install Node dependencies (for internal tools)
npm install
```

---

## ⚙️ Configuration

Create a `.env` file in the root directory:

```env
MONGO_URL=mongodb://localhost:27017
DB_NAME=xmunim_app
MSG91_AUTH_KEY=your_auth_key
MSG91_TEMPLATE_ID=your_template_id
FIREBASE_SERVICE_ACCOUNT_JSON='{...}'
```

---

## ▶️ Run Server

```bash
# Development mode with reload
python -m uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```

Server will start at: `http://localhost:8000`
Swagger UI Documentation: `http://localhost:8000/docs`

---

## 🏗️ Data Models (Pydantic)

- **User**: Phone, name, active_role, admin_roles, verified status.
- **Shop**: Name, category, location, shop_code, owner_id.
- **Customer**: Links a user to a shop with a balance.
- **Product**: Items sold by a shop (name, price, active status).
- **Transaction**: Credit or Payment records with product list and notes.
- **PaymentRequest**: Log of notifications and scheduled reminders.

---

## ⏰ Background Workers

The backend includes a persistent worker (`reminder_worker`) that runs every 60 seconds:
1. **Scheduled Reminders**: Processes one-off notifications set for specific dates/times.
2. **Auto-Reminder Engine**: Scans for overdue balances and sends alerts based on customer preferences (Daily, Weekly, etc.).
3. **TTL Cleanup**: MongoDB indexes automatically expire notification logs after 30 days.

---

## 📡 API Reference

All API endpoints other than public ones require an `Authorization: Bearer <token>` header.

### 🔐 Authentication & Profile
- `POST /api/auth/send-otp`: Request OTP for login/signup (`phone`, `name`, `is_login`).
- `POST /api/auth/verify-otp`: Validate OTP and get JWT token (`phone`, `otp`).
- `GET /api/auth/me`: Get current user profile.
- `PUT /api/auth/me`: Update profile fields (`name`, `fcm_token`, `push_enabled`, etc.).
- `POST /api/auth/me/photo`: Upload base64 profile photo (`photo`).
- `DELETE /api/auth/me/photo`: Remove profile photo.
- `POST /api/auth/switch-role`: Change `active_role` (`role`).

### 🛡️ Sessions & Data Export
- `GET /api/auth/sessions`: List all active login sessions.
- `POST /api/auth/logout`: Logout current session.
- `POST /api/auth/logout-all`: Logout all active sessions for the user.
- `POST /api/auth/request-data-export`: Generate signed PDF of personal data.

### 🏪 Shop Management (Shop Owner)
- `GET /api/shops`: List shops owned by user.
- `POST /api/shops`: Create a new shop (`name`, `category`, `pincode`, etc.).
- `GET /api/shops/{id}/dashboard`: Get stats (revenue, pending dues, recent txs).
- `PUT /api/shops/{id}`: Update shop details.

### 👥 Customer Management
- `GET /api/shops/{id}/customers`: List all shop customers with balances and filter by date.
- `POST /api/shops/{id}/customers`: Add a new customer (`name`, `phone`).
- `PUT /api/shops/{id}/customers/{customer_id}`: Update customer preferences.
- `POST /api/shops/{id}/customers/{customer_id}/send-verification`: Generate verification deep link.

### 📦 Product Inventory
- `GET /api/shops/{id}/products`: List all active products.
- `POST /api/shops/{id}/products`: Create new product (`name`, `price`).
- `PUT /api/shops/{id}/products/{product_id}`: Update price or status.
- `DELETE /api/shops/{id}/products/{product_id}`: Remove product.

### 💰 Transactions & Ledger
- `GET /api/shops/{id}/transactions`: List all transactions in a shop.
- `POST /api/shops/{id}/transactions`: Record Credit or Payment (`customer_id`, `type`, `amount`, `products`).
- `GET /api/customer/ledger`: (Customer) View personal ledger across all shops.

### 🔔 Notifications & Reminders
- `POST /api/shops/{id}/customers/{customer_id}/notify-payment`: Send push/SMS notification (`title`, `body`).
- `GET /api/shops/{id}/notifications`: View shop notification history.
- `GET /api/customer/notifications`: (Customer) View history of payment requests received.

### 🔑 Admin Panel
- `GET /api/admin/dashboard`: Global metrics (total users, active shops, revenue).
- `GET /api/admin/users`: Search and manage all app users.
- `PUT /api/admin/users/{id}`: Verify/unverify or flag users.
- `GET /api/admin/shops`: Global shop list with owner details.
- `GET /api/admin/transactions`: Global transaction history.
- `POST /api/admin/assign-role`: Grant/revoke Admin or Super Admin roles.

### 🌍 Public Access (No Auth)
- `GET /api/public/verify-customer/{id}`: HTML Verification Page for customers.
- `POST /api/public/verify-customer/{id}`: Mark customer as verified via API.
- `GET /api/shops/public/{shop_code}`: Get shop details by code.
- `POST /api/shops/public/{shop_code}/connect`: Connect customer to shop via QR/Link.

---

## 🔐 Security & Roles

- **Customer**: Default role. Can view personal ledger and manage profile.
- **Shop Owner**: Can create/manage shops, customers, and records.
- **Admin**: Can view system stats and manage user verification status.
- **Super Admin**: Full access including role management (granting Admin access).

---

## 👨‍💻 Author

Developed with ❤️ for XMunim
