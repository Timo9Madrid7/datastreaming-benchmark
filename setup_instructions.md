**Project Setup Instructions for Windows + VS Code**

This document provides step-by-step instructions to prepare a Windows machine for running and developing this project using **Visual Studio Code**, **Docker**, and necessary C++ tools.

---

## **1. Install Required Tools**

### **1.1 Install Visual Studio Code**
- Download and install **VS Code** from: [https://code.visualstudio.com/](https://code.visualstudio.com/)
- Open VS Code and install the following extensions:
  - **C/C++** (by Microsoft) → Provides IntelliSense & debugging
  - **CMake Tools** (if using CMake)
  - **Docker** (for Docker integration in VS Code)

### **1.2 Install `vcpkg` (C++ Package Manager)**
- Open **PowerShell** (Run as Administrator) and run:
  ```powershell
  git clone https://github.com/microsoft/vcpkg.git
  cd vcpkg
  .\bootstrap-vcpkg.bat
  ```
- To integrate `vcpkg` with VS Code, run:
  ```powershell
  .\vcpkg.exe integrate install
  ```

### **1.3 Install Microsoft C++ Compiler (MSVC)**
- Download **Visual Studio Build Tools** from:
  [https://visualstudio.microsoft.com/downloads/](https://visualstudio.microsoft.com/downloads/)
- Install the following components:
  - **MSVC v143 - VS 2022 C++ x64/x86 build tools**
  - **Windows 10 SDK**
  - **C++ CMake tools for Windows** (optional, if using CMake)
- Verify installation by running:
  ```powershell
  where cl
  ```
  Expected output: a path to `cl.exe` (e.g., `C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\<version>\bin\Hostx64\x64\cl.exe`)

---

## **2. Install and Configure Docker**

### **2.1 Install Docker Desktop**
- Download and install **Docker Desktop** from:
  [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
- During installation, select **WSL 2** as the default backend.
- Restart your computer if prompted.

### **2.2 Verify Docker Installation**
- Open **PowerShell** and run:
  ```powershell
  docker --version
  ```
  Expected output: `Docker version <number>, build <hash>`
- Test Docker with:
  ```powershell
  docker run hello-world
  ```
  Expected output: `Hello from Docker!` message.

---

## **3. Configure System Environment Variables**

### **3.1 Add `vcpkg` to System Path** (Optional, for easier access)
1. Open **Start Menu** → Search for **“Edit the system environment variables”**.
2. Click **Environment Variables**.
3. Under **System Variables**, find and edit `Path`.
4. Click **New**, then add:
   ```
   C:\path\to\vcpkg
   C:\path\to\vcpkg\installed\x64-windows\bin
   ```
5. Click **OK** and restart your computer.

### **3.2 Add Docker’s `bin` Folder to System Path** (Optional, if needed)
- Ensure `C:\Program Files\Docker\Docker\resources\bin` is added to `Path`.
- Restart your computer after changes.

---

## **4. Verify the Setup**

After completing the steps above, run the following checks:
- **Check compiler**: `cl /?`
- **Check vcpkg**: `vcpkg --version`
- **Check Docker**: `docker --version`
- **Check VS Code**: Open VS Code and confirm extensions are installed.

Once all these checks pass, your system is fully set up for development! 🚀

