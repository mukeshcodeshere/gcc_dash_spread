# 🛢️ Dash Spread Calculator – Installation Guide

This guide will walk you through setting up and running **Dash Spread Calculator** step-by-step.

---

## 📥 Step 1: Install Git

1. Download Git from:
   👉 [https://git-scm.com/downloads](https://git-scm.com/downloads)
2. Follow the installation instructions for your operating system.

---

## 📥 Step 2: Install Anaconda

1. Download Anaconda from:
   👉 [https://www.anaconda.com/products/distribution](https://www.anaconda.com/products/distribution)
2. Follow the installation instructions for your operating system.

---

## 📁 Step 3: Navigate to Your Documents Folder

Before cloning the repository, navigate to your **Documents** folder where you want to store the project.

1. Open **Anaconda Prompt** (on Windows) or your **terminal** (on macOS/Linux).
2. Run the following command to go to your Documents folder:

   **For Windows:**

   ```bash
   cd %USERPROFILE%\Documents
   ```

   **For macOS/Linux:**

   ```bash
   cd ~/Documents
   ```

This ensures that the project is stored in your **Documents** folder.

---

## 📂 Step 4: Clone the Project Repository

Now that you're in your **Documents** folder:

1. Run the following command to clone the repository:

   ```bash
   git clone https://github.com/mukeshcodeshere/gcc_dash_spread.git
   ```

2. Navigate into the project folder:

   ```bash
   cd gcc_dash_spread
   ```

   The project will now be stored in your **Documents** folder, inside the `gcc_dash_spread` directory.

---

## 🐍 Step 5: Create & Activate Your Python Environment

In Anaconda Prompt (or terminal), run these commands to create and activate a new environment:

```bash
conda create --name work python=3.13.2
conda activate work
```

This sets up a clean Python environment named `work`.

---

## 📦 Step 6: Install Project Dependencies

Make sure you're inside the `gcc_dash_spread` folder, then install the required dependencies by running:

```bash
pip install -r requirements.txt
```

---
 ## Step 7: Environment Variable Setup Instructions

### 🔐 Environment Variables Setup

To securely store and use sensitive credentials (like database connection info), create a file named `.env` in the root directory of the project.

**Steps:**

1. Create a `credential.env` file in the 'gcc_dash_spread' folder.
 
2. Add the following content to the `credential.env` file (replace values as needed):

```env
DB_SERVER=XXX - DB login item
DB_NAME=XXX - DB login item
DB_USERNAME=XXX - DB login item
DB_PASSWORD=XXX - DB login item
USERNAME_LOGIN=XXX - MV login item
PASSWORD_LOGIN=XXX - MV login item
GvWSUSERNAME=GCC018 - GvWS login item
GvWSPASSWORD=password - GvWS login item
```

3. **Do not upload or commit this credential.env file** 

The application will automatically load these environment variables at runtime using `python-dotenv`.

---

## ▶️ Step 7: Run the Application

To start the application, run the following command in your terminal:

```bash
python dash_launcher.py
```

You will be prompted to enter your calculation mode.

---

## ✅ Success!

Once you've successfully logged in, the application will open in your **web browser** at:

👉 [http://localhost:8050](http://localhost:8050)


---

### 🔴 To Stop the Application

* **Windows**: In the Anaconda Prompt, press `Ctrl  C`
* **macOS/Linux**: In the Terminal, press `Cmd  C`

This will shut down the application.

---

If you need help, please reach out to the Analysts.

---


## ▶️ Step X: To update presets

To update the presets, modify the PriceAnalyzeIn.csv with all the presets you want and run the following command in your terminal:

```bash
python PriceBuilding_v101.py