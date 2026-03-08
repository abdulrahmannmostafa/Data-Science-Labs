@echo off

echo Go to the project directory...
cd "Lab 3/src/Integrated Data Pipeline"

echo Installing requirements...
pip install -r requirements.txt

echo Seeding database...
python seed.py

echo Running pipeline...
python final_project.py

echo Pipeline finished!
pause