# 🏪 ShopMunim Backend

Backend API server for ShopMunimApp built with **FastAPI** and **MongoDB**.

---

## 🛠️ Tech Stack

- FastAPI
- MongoDB (Motor - Async Driver)
- JWT Authentication
- Pydantic

---

## 📦 Prerequisites

- **Python** (v3.9+) - [Download](https://python.org/)
- **MongoDB** - [Download](https://mongodb.com/try/download/community)

---

## 🚀 Installation

```bash
# Clone the repository
git clone https://github.com/your-username/ShopMunim-Backend.git
cd Shopmunim-Backend

# Create virtual environment
python -m venv venv

# Activate virtual environment (Windows)
venv\Scripts\activate

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
python -m uvicorn server:app --host 0.0.0.0 --port 8000
```

Server will start at: `http://localhost:8000`

API Documentation: `http://localhost:8000/docs`

---

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/send-otp` | Send OTP |
| POST | `/api/auth/verify-otp` | Verify OTP & Login |
| GET | `/api/auth/me` | Get Current User |
| POST | `/api/shops` | Create Shop |
| GET | `/api/shops` | Get My Shops |
| GET | `/api/shops/{id}/customers` | Get Customers |
| POST | `/api/shops/{id}/transactions` | Create Transaction |

---

## 👨‍💻 Author

Developed with ❤️ for ShopMunim
