# HealthlineMedia

#Requirements
pip install boto3
pip install awscli
 
#Then configure your creds
#aws configure

#Examples
python logparser.py getcodes --from 2017/08/01 --to 2017/08/07 --max 100
python logparser.py getcodes --from 2017/08/01 --to 2017/08/01
python logparser.py getcodes --for 2 months --max 100
python logparser.py geturls --from 2017/08/01 --to 2017/08/07 --max 100
python logparser.py geturls --code 404 --from 2017/08/01 --to 2017/08/07 --max 100
python logparser.py getUAs --code 404 --from 2017/08/01 --to 2017/08/07 --max 100
python logparser.py getreport --code 404 --from 2017/08/01 --to 2017/08/07 --max 100
