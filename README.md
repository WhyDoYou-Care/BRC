# **50 Shades of Segfault**  

This repository is the **boilerplate** for this Challenge. Follow these 13 instructions carefully to set up your environment, submit your solution, and compete for the top spot on the leaderboard.  

```
THE INSTRUCTIONS BELOW ARE FOR COMPLETE BEGINNERS AND RESULTS IN THE FORMATION OF A PUBLIC REPOSITORY.
FOR PEOPLE WITH A SOMEWHAT DECENT UNDERSTANDING OF GIT AND GITHUB, FOLLOWING STEP BY STEP MIGHT NOT BE NECESSARY.
CLONING AND SETTING UP YOUR OWN PRIVATE REPOSITORY MIGHT BE A BETTER ALTERNATIVE.
AND YES THE ENTIRE FLOW WILL BE ABLE TO DETECT AND HANDLE YOUR PRIVATE REPOSITORIES AS WELL!
```

## **0. Install the GitHub App (Mandatory Before Registering)**

Before you register, you must install the 50 Shades of Segfault GitHub App on your profile. This allows us to track your repository and validate your submissions.

**[Github - 50 Shades of Segfault Prod App](https://github.com/apps/50-shades-of-segfault-prod-app)**


## **1. Fork the Repository**  

Before registering, you must **fork** the boilerplate repository.  

- Go to the top of **[this repo](https://github.com/SteakFisher/BRC)**.  
- Click the **Fork** button in the top-right corner.  
- Once the repository is forked, it will appear under your GitHub account at:  
   ```
   https://github.com/YOUR_GITHUB_USERNAME/BRC
   ```
   Replace `YOUR_GITHUB_USERNAME` with your actual GitHub username.
  
   [ You can also manually navigate to it, via the repositories section section under your GitHub Profile ]

## **2. Register for the Challenge**  

After forking, register at:  

**[https://50sos.vercel.app](https://50sos.vercel.app)**  

During registration, you will need to **choose your forked repository**. This is the repository where you will push your solutions.  

## **3. Clone Your Forked Repository**  

### **Linux / macOS / Windows (Git Bash / PowerShell / CMD)**
```cmd
git clone https://github.com/YOUR_GITHUB_USERNAME/BRC.git
cd BRC
```

Replace `YOUR_GITHUB_USERNAME` with your actual GitHub username.

[ A basic knowledge of git clone, commit and push commands would greatly help you. You aren't MANDATED to use the commands, so long as you can push the changes to github, the tests will run ]

## **4. Install & Set Up Docker**  

Docker is required to run the testing environment locally.  

### **Install Docker Desktop**  
Download and install Docker Desktop from:  
[https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)  

### **Verify Docker Installation**  
Run the following command to ensure Docker is installed and running:  

```sh
docker --version
```

If installed correctly, it should return a something like this:  
```
Docker version 24.0.5, build ced0996
```

ALWAYS ensure Docker Desktop is **running** before proceeding.  

## **5. Writing Your Solution**  

- Modify **`main.py`** inside the `src/` folder (no other language allowed ofc).  
- **Do NOT** create files outside the `src/` folder.  
- **Only standard Python libraries are allowed**. Do **NOT** use `pip install`.  
- **Your program must read input from `testcase.txt`** (which will be auto-generated when you execute the docker compose command) **and write output to `output.txt`.**  

## **6. Input & Output Format**  

### **Input (`testcase.txt`)**
Each line of the input file follows this format:  
```
{city};{idliconsumptionscoreofthatcity}
```

Example:
```
Lulla-Nagar;76.2
Patiala;-23.0
Nashik;58.5
Kollam;92.2
Vapi;25.0
```

- Each city may **appear multiple times** in the input file.  
- Your program must **compute the min, mean, and max for each city**.  

### **Output (`output.txt`)**
- **Cities must be sorted alphabetically**.  
- **Output format:**  
  ```
  {city}={min_temp}/{mean_temp}/{max_temp}
  ```
- The event follows **IEEE 754 "round to infinity" standard** (one decimal place).  

#### **Example Output**
```
Kollam=92.2/92.2/92.2
Lulla-Nagar=76.2/76.2/76.2
Nashik=58.5/58.5/58.5
Patiala=-23.0/-23.0/-23.0
Vapi=25.0/25.0/25.0
```

## **7. Running Your Code Locally**  

To test your solution, use the `LEVEL` environment variable:  

### **Linux / macOS / Git Bash (Windows)**
```sh
LEVEL=1 docker compose up --build
```

### **Windows (PowerShell)**
```powershell
$env:LEVEL="1"; docker compose up --build
```

### **Windows (Command Prompt - CMD)**
```cmd
set LEVEL=1
docker compose up --build
```

- `LEVEL=n` means **n million rows** in the `testcase.txt` file.  
- You can **test on any LEVEL** locally. ( Decimal values are accepted, LEVEL=0.1 will test your output on 100,000 values )

## **8. Test-Submitting Your Solution**  

### **Push Your Code to GitHub**  

Once your solution is ready, push your changes to GitHub.  

```sh
git add .
git commit -m "Updated solution"
git push origin main
```

This will trigger an **automated test** on your GitHub repository using a **benchmark machine (2 vCPU + 8GB RAM) with LEVEL=5**.  

Check the results on:  
- Your **GitHub repository page**  
- **[https://50sos.vercel.app/status](https://50sos.vercel.app/status)**  

These **push tests do not count towards the leaderboard**—they are just for your reference.  

## **9. Submitting for the Leaderboard**  

Once satisfied, go to:  

[https://50sos.vercel.app/status](https://50sos.vercel.app/status)  

Find your commit and click **Upgrade**.  

- This runs your code on a **higher-spec machine (4 vCPU + 16GB RAM) with LEVEL=15**.  
- **Only upgraded submissions count towards the leaderboard.**  
- **You can request only ONE commit upgrade per hour.**  
- **You cannot request an upgrade within the first hour of creating your account.**  
- If you upgrade multiple commits, **the one with the fastest runtime will be considered for the leaderboard**.  

## **10. Execution Time Limits & Rules**  

- **Maximum execution time:** **40 seconds**  
- If your code takes **more than 40 seconds**, it will be **terminated** and marked as **failed**.  
- **Push tests:** Run with `LEVEL=5` (for your reference).  
- **Upgrade tests:** Run with `LEVEL=15` (used for the leaderboard).  

## **11. Winning Criteria**  

- The **fastest valid execution** on `LEVEL=15` determines the winner.  
- **Only Python standard libraries are allowed.**  
- **No precompiled binaries, no external dependencies, no API calls unless explicitly allowed.**  

## **12. Full Rulebook & Support**  

The complete **rulebook** can be found here:  
**[RULES.md](https://github.com/SteakFisher/BRC/blob/master/RULES.md)**

For any questions or issues, you can directly contact the **50 Shades of Segfault** organizing team:  

- **Jaydeep Bejoy:** [202351165@iiitvadodara.ac.in](mailto:202351165@iiitvadodara.ac.in) or [+919074755597](tel:+919074755597)
- **Devyash Saini:** [202351030@iiitvadodara.ac.in](mailto:202351030@iiitvadodara.ac.in) or [+919531852385](tel:+919531852385)


---
Now go forth and conquer big data!
