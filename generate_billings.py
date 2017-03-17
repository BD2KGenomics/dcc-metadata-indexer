from datetime import datetime
from active_alchemy import ActiveAlchemy
from elasticsearch import Elasticsearch
from decimal import Decimal
import pytz
import calendar
import json
import os

db = ActiveAlchemy(os.environ['DATABASE_URL'])
es_service = os.environ.get("ES_SERVICE", "localhost")
es = Elasticsearch(['http://'+es_service+':9200/'])
pricing = json.load(open("region_instance_prices.json"))
EXTRA_MONEY = 1.2  # if you want to tune billings, this is the dial. 1.2 means add 20% on top of what is calculated
# that we pay to AWS or whichever host
SECONDS_IN_HR = 3600

BYTES_IN_GB = 1000000000
STORAGE_PRICE_GB_MONTH = 0.03

class Billing(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    storage_cost = db.Column(db.Numeric, nullable=False, default=0)
    compute_cost = db.Column(db.Numeric, nullable=False, default=0)
    project = db.Column(db.Text)
    start_date = db.Column(db.DateTime)
    end_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime, default=datetime.utcnow())
    closed_out = db.Column(db.Boolean, nullable=False, default=False)
    cost_by_analysis = db.Column(db.JSON)
    __table_args__ = (db.UniqueConstraint('project', 'start_date', name='unique_prj_start'),)

    def __init__(self, compute_cost, storage_cost, project, cost_by_analysis, start_date, end_date, **kwargs):
        db.Model.__init__(self, compute_cost=compute_cost, storage_cost=storage_cost, project=project,
                          start_date=start_date.replace(tzinfo=pytz.UTC),
                          end_date=end_date.replace(tzinfo=pytz.UTC),
                          cost_by_analysis=cost_by_analysis,
                        **kwargs)

    def __repr__(self):
        return "<Billing, Project: {} , Cost: {}, Time Range: {}-{}, Time created: {}".format(
                self.project, self.cost, str(self.start_date),
                str(self.end_date), str(self.created_date))

    def to_json(self):
        dict_representation = {}
        dict_representation["cost"] = str(round(self.cost,2))
        dict_representation["compute_cost"] = str(round(self.compute_cost, 2))
        dict_representation["storage_cost"] = str(round(self.storage_cost,2))
        dict_representation["project"] = self.project
        dict_representation["start_date"] = datetime.strftime(self.start_date, format="%a %b %d %H:%M:%S %Z %Y")
        dict_representation["end_date"] = datetime.strftime(self.end_date, format="%a %b %d %H:%M:%S %Z %Y")
        dict_representation["by_analysis"] = self.cost_by_analysis
        dict_representation["month_of"] = datetime.strftime(self.start_date, format="%B-%Y")
        return dict_representation

    def __close_out__(self):
        self.end_date = datetime.utcnow().replace(tzinfo=pytz.UTC)
        self.closed_out = True

    @property
    def cost(self):
        return self.compute_cost+self.storage_cost

def get_projects_list():
    es_resp = es.search(index='billing_idx', body={"query": {"match_all": {}}, "aggs": {
        "projects":{
            "terms":{
                "field": "project.keyword",
                "size": 9999
            }
        }
    }}, size=0)

    projects = []
    for project in es_resp['aggregations']['projects']['buckets']:
        projects.append(project['key'])
    return projects

def get_previous_file_sizes (timeend, project):
    timeendstring = timeend.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = es.search(index='billing_idx', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "project.keyword": project
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "lt": timeendstring,
                            }
                        }
                    }
                ]

            }
        },
        "aggs": {
            "filtered_nested_timestamps": {
                "nested": {
                    "path": "specimen.samples.analysis"
                },
                "aggs": {
                    "sum_sizes": {
                        "sum": {
                            "field": "specimen.samples.analysis.workflow_outputs.file_size"
                        }
                    }
                }
            }
        }
    }, size=9999)
    return es_resp

def get_months_uploads(project, timefrom, timetil):
    timestartstring = timefrom.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S')
    timeendstring = timetil.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = es.search(index='billing_idx', body =
    {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": timestartstring,
                                "lt": timeendstring
                            }
                        }
                    },
                    {
                        "term": {
                            "project.keyword": project
                        }
                    }
                ]
            }
        },
        "aggs": {
            "filtered_nested_timestamps": {
                "nested": {
                    "path": "specimen.samples.analysis"
                },
                "aggs": {
                    "times": {
                        "terms": {
                            "field": "specimen.samples.analysis.timestamp"
                        },
                        "aggs": {
                            "sum_sizes": {
                                "sum": {
                                    "field": "specimen.samples.analysis.workflow_outputs.file_size"
                                }
                            }
                        }
                    }
                }
            }
        }
    }, size=9999)
    return es_resp

def make_search_filter_query(timefrom, timetil, project):
    """

    :param timefrom: datetime object, filters all values less than this
    :param timetil: datetime object, filters all values greater than or equal to this
    :param project: string, this is the name of the particular project that we are trying to generate for
    :return:
    """
    timestartstring = timefrom.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S')
    timeendstring = timetil.replace(tzinfo=pytz.UTC).strftime('%Y-%m-%dT%H:%M:%S')
    es_resp = es.search(index='billing_idx', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "project.keyword": project
                        }
                    },
                    {
                        "nested": {
                            "path": "specimen.samples.analysis",
                            "score_mode": "max",
                            "query": {
                                "range": {
                                    "specimen.samples.analysis.timing_metrics.overall_stop_time_utc": {
                                        "gte": timestartstring,
                                        "lt": timeendstring,
                                        "format": "yyy-MM-dd'T'HH:mm:ss"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "filtered_nested_timestamps": {
                "nested": {
                    "path": "specimen.samples.analysis"
                },
                "aggs": {
                    "filtered_range": {
                        "filter": {
                            "range": {
                                "specimen.samples.analysis.timing_metrics.overall_stop_time_utc": {
                                    "gte": timestartstring,
                                    "lt": timeendstring,
                                    "format": "yyy-MM-dd'T'HH:mm:ss"
                                }}
                        },
                        "aggs": {
                            "vmtype": {
                                "terms": {
                                    "field": "specimen.samples.analysis.host_metrics.vm_instance_type.raw",
                                    "size": 9999
                                },
                                "aggs": {
                                    "regions": {
                                        "terms": {
                                            "field": "specimen.samples.analysis.host_metrics.vm_region.raw",
                                            "size": 9999
                                        },
                                        "aggs": {
                                            "totaltime": {
                                                "sum": {
                                                    "field": "specimen.samples.analysis.timing_metrics.overall_walltime_seconds"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }, size=9999)

    return es_resp

def get_datetime_from_es(timestr):
    return datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=pytz.UTC)

def calculate_compute_cost(total_seconds, vm_cost_hr):
    return Decimal(Decimal(total_seconds)/Decimal(SECONDS_IN_HR)*Decimal(vm_cost_hr)*Decimal(EXTRA_MONEY))

def calculate_storage_cost(portion_month_stored, file_size_gb):
    return Decimal(portion_month_stored)*Decimal(file_size_gb)*Decimal(STORAGE_PRICE_GB_MONTH)*Decimal(EXTRA_MONEY)



def get_vm_string(host_metrics):
    return str(host_metrics.get("vm_region")) + str(host_metrics.get("vm_instance_type"))


def make_bills(comp_aggregations, previous_month_bytes, portion_of_month, this_month_timestamps_sizes, curr_time,
               seconds_in_month):
    x=comp_aggregations
    print(x)
    instances = x["aggregations"]["filtered_nested_timestamps"]["filtered_range"]["vmtype"]["buckets"]
    total_pricing = Decimal()
    for instance in instances:
        instanceType = instance["key"]
        regions = instance["regions"]["buckets"]
        for region in regions:
            regionName = region["key"]
            totalTime = region["totaltime"]["value"]
            print(regionName, instanceType, totalTime, pricing[regionName+instanceType])
            total_pricing += calculate_compute_cost(totalTime, pricing[regionName + instanceType])

    # need to get the storage size for files completed before start of this month
    storage_size_bytes = previous_month_bytes['aggregations']['filtered_nested_timestamps']['sum_sizes']['value']
    storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)
    total_pricing += Decimal(STORAGE_PRICE_GB_MONTH)*storage_size_gb*portion_of_month*Decimal(EXTRA_MONEY)


    # calculate the money spent on storing workflow outputs which were uploaded during this month
    this_month_timestamps = this_month_timestamps_sizes['aggregations']['filtered_nested_timestamps']['times'][
        'buckets']
    for ts_sum in this_month_timestamps:
        time_string = ts_sum['key_as_string']
        time = datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)

        timediff = (curr_time - time).total_seconds()
        month_portion = Decimal(timediff)/Decimal(seconds_in_month)

        storage_size_bytes = ts_sum['sum_sizes']['value']
        storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)

        cost_here = storage_size_gb * month_portion
        total_pricing += cost_here

    return total_pricing


def get_compute_costs(comp_aggregations):
    #create total compute cost for an entire project for a month
    instances = comp_aggregations["aggregations"]["filtered_nested_timestamps"]["filtered_range"]["vmtype"]["buckets"]
    compute_costs = Decimal(0)
    for instance in instances:
        instanceType = instance["key"]
        regions = instance["regions"]["buckets"]
        for region in regions:
            regionName = region["key"]
            totalTime = region["totaltime"]["value"]
            print(regionName, instanceType, totalTime, pricing[regionName+instanceType])
            compute_costs += calculate_compute_cost(totalTime, pricing[regionName + instanceType])
    return compute_costs


def create_analysis_costs_json(this_month_comp_hits, bill_time_start, bill_time_end):
    analysis_costs = []
    analysis_cost_actual = 0
    for donor_doc in this_month_comp_hits:
        donor = donor_doc.get("_source")
        for specimen in donor.get("specimen"):
            for sample in specimen.get("samples"):
                for analysis in sample.get("analysis"):
                    timing_stats = analysis.get("timing_metrics")
                    if timing_stats:
                        time = timing_stats["overall_walltime_seconds"]
                        analysis_end_time = get_datetime_from_es(timing_stats["overall_stop_time_utc"])
                        analysis_start_time = get_datetime_from_es(timing_stats["overall_start_time_utc"])
                        if analysis_end_time < bill_time_end and analysis_start_time >= bill_time_start:
                            host_metrics = analysis.get("host_metrics")
                            if host_metrics:
                                cost = calculate_compute_cost(time, pricing.get(get_vm_string(host_metrics)))
                                analysis_costs.append(
                                    {
                                        "donor": donor.get("submitter_donor_id"),
                                        "specimen": specimen.get("submitter_specimen_id"),
                                        "sample": sample.get("submitter_sample_id"),
                                        "workflow": analysis.get("analysis_type"),
                                        "version": analysis.get("workflow_version"),
                                        "cost": str(cost)
                                    }
                                )
                                analysis_cost_actual += cost

    return analysis_costs


def workflow_output_total_size(workflow_outputs_array):
    size = 0
    if workflow_outputs_array:
        for output in workflow_outputs_array:
            this_size = output.get("file_size")
            if this_size:
                size+=this_size
    return size

def get_gb_size(byte_size):
    return Decimal(byte_size)/Decimal(BYTES_IN_GB)

def create_storage_costs_json(project_files_hits, bill_time_start, bill_time_end, month_total_seconds):

    storage_costs = []
    storage_cost_actual = 0
    for donor_doc in project_files_hits:
        donor = donor_doc.get("_source")
        for specimen in donor.get("specimen"):
            for sample in specimen.get("samples"):
                for analysis in sample.get("analysis"):
                    timing_stats = analysis.get("timing_metrics")
                    if timing_stats:
                        analysis_end_time = get_datetime_from_es(timing_stats["overall_stop_time_utc"])
                        if analysis_end_time < bill_time_end:
                            this_size = get_gb_size(workflow_output_total_size(analysis.get("workflow_outputs")))
                            if analysis_end_time >= bill_time_start: #means it's from this month
                                seconds = (bill_time_end - analysis_end_time).total_seconds()
                            else:#it's from previous month, charge it portion of month
                                seconds = (bill_time_end - bill_time_start).total_seconds()
                            cost = calculate_storage_cost(Decimal(seconds)/Decimal(month_total_seconds), this_size)
                            storage_costs.append(
                                {
                                    "donor": donor.get("submitter_donor_id"),
                                    "specimen": specimen.get("submitter_specimen_id"),
                                    "sample": sample.get("submitter_sample_id"),
                                    "workflow": analysis.get("analysis_type"),
                                    "version": analysis.get("workflow_version"),
                                    "cost": str(cost)
                                }
                            )
                            storage_cost_actual += cost

    return storage_costs

def get_storage_costs(previous_month_bytes, portion_of_month, this_month_timestamps_sizes, curr_time, seconds_in_month):
    storage_costs = Decimal(0)
    storage_size_bytes = previous_month_bytes['aggregations']['filtered_nested_timestamps']['sum_sizes']['value']
    storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)
    storage_costs += calculate_storage_cost(portion_of_month, storage_size_gb)

    # calculate the money spent on storing workflow outputs which were uploaded during this month
    this_month_timestamps = this_month_timestamps_sizes['aggregations']['filtered_nested_timestamps']['times'][
        'buckets']
    for ts_sum in this_month_timestamps:
        time_string = ts_sum['key_as_string']
        time = datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)

        timediff = (curr_time - time).total_seconds()
        month_portion = Decimal(timediff)/Decimal(seconds_in_month)

        storage_size_bytes = ts_sum['sum_sizes']['value']
        storage_size_gb = Decimal(storage_size_bytes)/Decimal(BYTES_IN_GB)

        storage_costs += calculate_storage_cost(month_portion, storage_size_gb)

    return storage_costs

def generate_daily_reports(date):
    # Need to pass app context around because of how flask works
    # can take a single argument date as follows
    # flask generate_daily_reports --date 2017/01/31 will compute the billings for jan 2017, up to the 31st day of
    # January

    try:
        timeend = datetime.strptime(date, '%Y/%m/%d').replace(tzinfo=pytz.UTC)
    except:
        timeend = datetime.utcnow().replace(tzinfo=pytz.UTC).replace(minute=0, second=0, hour=0, microsecond=0)


    # HANDLE CLOSING OUT BILLINGS at end of month
    if timeend.day == 1:
        projects = get_projects_list()
        for project in projects:
            bill = Billing.query.filter(Billing.end_date.month == (timeend.month-1) % 12) \
                .filter(Billing.closed_out is False).filter(Billing.project == project).first()
            if bill:
                bill.update(end_date=timeend, closed_out=True)


    monthstart = timeend.replace(day=1)
    projects = get_projects_list()
    seconds_into_month = (timeend-monthstart).total_seconds()
    daysinmonth = calendar.monthrange(timeend.year, timeend.month)[1]
    portion_of_month = Decimal(seconds_into_month)/Decimal(daysinmonth*3600*24)

    for project in projects:
        print(project)
        file_size = get_previous_file_sizes(monthstart, project=project)
        this_months_files = get_months_uploads(project, monthstart, timeend)
        compute_cost_search =  make_search_filter_query(monthstart,timeend,project)
        compute_costs = get_compute_costs(compute_cost_search)
        analysis_compute_json = create_analysis_costs_json(compute_cost_search['hits']['hits'], monthstart, timeend)

        all_proj_files = get_previous_file_sizes(timeend, project)['hits']['hits']
        analysis_storage_json = create_storage_costs_json(all_proj_files, monthstart, timeend, daysinmonth*3600*24)
        storage_costs = get_storage_costs( file_size, portion_of_month,
                                            this_months_files, timeend, daysinmonth*3600*24)

        bill = Billing.query().filter(Billing.project == project).filter(Billing.start_date.month == monthstart.month).first()
        itemized_costs = {
            "itemized_compute_costs": analysis_compute_json,
            "itemized_storage_costs": analysis_storage_json
        }
        if bill:
            bill.update(compute_cost=compute_costs, storage_cost=storage_costs, end_date=timeend,
                        cost_by_analysis=itemized_costs)
        else:
            Billing.create(compute_cost=compute_costs, storage_cost=storage_costs, start_date=monthstart, \
                            end_date=timeend, project=project, closed_out=False,
                            cost_by_analysis=itemized_costs)

if __name__ == '__main__':
	generate_daily_reports("")
