# End-to-End Data Project

## Instructions
- Clone this repository
- Create a python virtual environment
`python -m venv env`
- Activate the environment
`source .\env\Scripts\activate`
- Install packages
`pip install -r requirements.txt`

## Overview
### [Part 1](https://www.youtube.com/watch?v=j7fNG-V4aGE)
Step 1: [Setup an FTP Server using WSL](https://tinyurl.com/wslconfig) <br> 
Step 2: Download `.csv` files from the web using Python<br>
Step 3: Upload downloaded `.csv` files to the FTP Server either manually or based on a schedule <br>

#### Running the app:
- Manually — `python app.py manual`
- Scheduled — `python app.py scheduled` <br> <br>
> ![carbon](https://github.com/user-attachments/assets/74c1c7bf-b01a-4045-9bc0-9d31e2c12d0b)

<br>

### [Part 2](https://www.youtube.com/watch?v=m2DD-RvT-nA)
Step 1: Create a pipeline in SSIS to:
- Download files from FTP Server, 
- Load it into PostgreSQL, and
- Merge the tables in PostgreSQL
> Entire Pipeline
> ![image](https://github.com/user-attachments/assets/6d860e8b-a67a-47dc-9f63-2a4cb12d5385)
<br><br> Merge OFAC Table Data Flow
> ![image](https://github.com/user-attachments/assets/985fd673-af91-4267-8df7-f390ae2d36db) 

<br>
Step 2: Deploy the Pipeline in SQL Server Agent and create an automated job <br> <br>
<blockquote>
  <img src="https://raw.githubusercontent.com/gryAI/End-to-End-Data-Project/main/img/deployment.png" width="400" />
</blockquote>










 
