####################
#                  #
# Authored BY : me #
#                  #  
####################

# Load Data sous format parquet ( pour ....)

import os
from datetime import datetime

def save_to_parquet(df, base_path="Flights/rawzone"):
    now = datetime.now()
    path = os.path.join(
        base_path,
        f"tech_year={now.year}",
        f"tech_month={now.strftime('%Y-%m')}",
        f"tech_day={now.strftime('%Y-%m-%d')}",
    )
    os.makedirs(path, exist_ok=True)
    filename = f"flights_{now.strftime('%Y%m%d%H%M%S')}.parquet"
    df.to_parquet(os.path.join(path, filename), index=False)
