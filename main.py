#requirements
import dimcli
import pandas as pd
import os
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import Forbidden
from google.oauth2 import service_account
import pandas_gbq

#dimcli auth with mat's api key
key=dimcli.login(key="your api key",
             endpoint="https://app.dimensions.ai")
#gcloud creds
credentials = service_account.Credentials.from_service_account_info(
    {add your credentials here},
)


print("Attempting login to dimensions api..")
print("logging in..")
dsl = dimcli.Dsl()
print(dsl)
print("logged in..")
# hindawi year queries
_2021 = dsl.query_iterative("""search publications where publisher ="Hindawi" and year=2021 return publications[date+date_original+altmetric+doi+issn+journal+journal+times_cited+recent_citations+relative_citation_ratio+research_org_country_names+research_org_names+supporting_grant_ids+year+volume+title] sort by times_cited desc""")
_2020 = dsl.query_iterative("""search publications where publisher ="Hindawi" and year=2020 return publications[date+date_original+altmetric+doi+issn+journal+journal+times_cited+recent_citations+relative_citation_ratio+research_org_country_names+research_org_names+supporting_grant_ids+year+volume+title] sort by times_cited desc""")

#dataframes
df_2021 = pd.DataFrame(_2021.publications)
df_2020 = pd.DataFrame(_2020.publications)

#frames from dataframes
frames = [df_2021, df_2020]
#squish them all together
result = pd.concat(frames)
#csv
#result.to_csv(r'')


destination_table = 'yourdataset.table'
project = 'yourproject'
result.to_gbq(destination_table, project_id=project, chunksize=None, reauth=False, if_exists='replace', auth_local_webserver=False, table_schema=None, location=None, progress_bar=True, credentials=credentials)

print("done.. hoorah!")
