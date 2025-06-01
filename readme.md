## Ready the environment (In Windows)
- python -m venv .venv
- .venv\Scripts\activate
- pip install -r requirements.txt

## init app
- virtualenv -p python3 .venv
- source .venv/bin/activate
- pip install -r requirements.txt

## run the code like below
python script.py --input_path='building4testing.csv' --class_1=HE800A --class_2=HE800A --lc='[101,102]'

## run reaction force comparison like below
python reaction.py --input_path_1='Blast-Rev04B.csv' --input_path_2='Blast-Rev05B.csv' --lc='[1,2]'