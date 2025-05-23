# 🛢️ Project Spartan – Installation Guide

This guide will walk you through setting up and running **Project Spartan** step-by-step.

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
   git clone https://github.com/mukeshcodeshere/project_spartan.git
   ```

2. Navigate into the project folder:

   ```bash
   cd project_spartan
   ```

   The project will now be stored in your **Documents** folder, inside the `project_spartan` directory.

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

Make sure you're inside the `project_spartan` folder, then install the required dependencies by running:

```bash
pip install -r requirements.txt
```

---
Here's your updated section incorporating the extended and refined information:

---

## ▶️ Step 7: Run the Application

To start the application, run the following command in your terminal:

```bash
python run_app.py
```

You will be prompted to enter your **MV Username and Password**.

### Possible Login Errors

There are two common issues you might encounter at this stage:

#### 🔹 1. Incorrect Credentials

* **What happens**: The app prompts you to log in again.
* **Fix**: Make sure you enter the correct username and password.

#### 🔹 2. MV Server Issue

* **What happens**: You entered valid credentials, but the login still fails due to a timeout issue on the MV server.
* **Fix**: Wait **2–3 minutes** and try again. This issue usually resolves itself shortly.

---

## ✅ Done!

Once login is successful, the application will automatically open in your **web browser**.

---

### 🔴 To Stop the Application

* **Windows**: In the Anaconda Prompt, press `Ctrl + C`
* **macOS/Linux**: In the Terminal, press `Cmd + C`

This will shut down the application.

---

If you need help, please reach out to the Analysts.

---