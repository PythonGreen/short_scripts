select 
date(starttime) as time_id
,e.userid
,e.slice
,e.tbl
,e.starttime
,e.session
,e.query
,trim(e.filename) filename
,e.line_number
,e.colname
,e.type
,e.col_length
,e.position
,trim(e.raw_line) raw_line
,trim(e.raw_field_value) raw_field_value
,e.err_code
,e.err_reason
from stl_load_errors e
where date(e.starttime)= current_date-1
and filename ilike 's3://olxgroup-reservoir-latam-bi/in/latam/hydra/%'
--and trim(e.filename) ilike 's3://olx-latam-bi/naspers-bi/olx/data-exported/latam_monetization_DW/%'
order by starttime desc 
limit 5000
