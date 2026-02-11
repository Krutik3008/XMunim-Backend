# 🏪 ShopMunim Backend

A robust, asynchronous API server for ShopMunimApp built with **FastAPI** and **MongoDB**. This backend serves the Frontend for Customers, Shop Owners, and Admins.

---

## 🛠️ Tech Stack

- **FastAPI**: Modern, high-performance web framework for building APIs with Python 3.9+.
- **MongoDB (Motor)**: Asynchronous driver for MongoDB.
- **Pydantic**: Data validation and settings management using Python type annotations.
- **JWT (Jose)**: Secure token-based authentication.
- **Uvicorn**: Lightning-fast ASGI server.
- **Dotenv**: Manage environment variables.

---

## 📦 Prerequisites

- **Python** (v3.9+) - [Download](https://python.org/)
- **MongoDB** - [Download](https://mongodb.com/try/download/community)

---

## 🚀 Installation

```bash
# Clone the repository
cd Shopmunim-Backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## ⚙️ Configuration

Create a `.env` file in the root directory:

```env
MONGO_URL=mongodb://localhost:27017
DB_NAME=shopmunim_app
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

---

## 📡 Key API Endpoints

### 🔐 Authentication
- `POST /api/auth/send-otp`: Mock OTP generation and sending.
- `POST /api/auth/verify-otp`: Validates OTP and returns JWT token.
- `GET /api/auth/me`: Returns profile of the current logged-in user.
- `POST /api/auth/switch-role`: Updates `active_role` for the user.

### 🏪 Shop Owner
- `POST /api/shops`: Create a new shop.
- `GET /api/shops`: List shops owned by the user.
- `GET /api/shops/{id}/customers`: List customers and their balances.
- `POST /api/shops/{id}/customers`: Add a new customer to a shop.
- `POST /api/shops/{id}/transactions`: record a new credit or payment.
- `GET /api/shops/{id}/products`: Manage shop inventory.

### 👤 Customer
- `GET /api/customer/ledger`: View ledger entries across different shops.
- `GET /api/customer/summary`: Get totals for credit, payment, and balance.
- `POST /api/customer/join-shop`: Link to a shop using a `shop_code`.

### 🔐 Admin
- `GET /api/admin/stats`: Global overview of users, shops, and transactions.
- `GET /api/admin/users`: Manage and search for all application users.
- `PUT /api/admin/users/{id}/verify`: Verify or flag users.
- `POST /api/admin/assign-role`: Grant or revoke Admin/Super Admin roles.
- `GET /api/admin/users-for-role-assignment`: Detailed user list for the Role Management screen.
  - *Includes `has_shop` field to identify shop owners regardless of current active role.*

---

## 🔐 Security & Roles

The system uses role-based access control (RBAC):
- **Customer**: Default role on registration.
- **Shop Owner**: Users who have created at least one shop.
- **Admin / Super Admin**: Users with elevated permissions, granted via the Assign Role endpoint.

---

## 👨‍💻 Author

Developed with ❤️ for ShopMunim