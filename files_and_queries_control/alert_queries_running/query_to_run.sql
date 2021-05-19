select 'cancel '||pid cancel_query,pid, trim(user_name) user_name,
	dateadd(hour,-3,starttime) starttime_local,
	trim(query) query,
	date_diff('s',starttime,sysdate) seconds_running
from stv_recents
where status='Running'
and  	date_diff('s',starttime,sysdate)  >= 3600
