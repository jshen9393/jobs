from etl.base.transformer import BaseTransformer


STAGE_TABLE_NAME = 'indeed_stage_jobs'
SOURCE_TABLE_NAME = 'indeed_jobs'

# <editor-fold desc='TSV fields'>
FIELDS = (
    'jobkey',
    'jobquery',
    'jobtitle',
    'company',
    'city',
    'state',
    'country',
    'latitude',
    'longitude',
    'language',
    'formattedlocation',
    'jobsource',
    'jobdate',
    'url',
    'onmousedown',
    'sponsored',
    'expired',
    'indeedapply',
    'formattedlocationfull',
    'formattedrelativetime',
    'stations',

)
# </editor-fold>

# <editor-fold desc='Stage table DDL'>
STAGE_TABLE_DDL = """
create table {} (
 	jobkey VARCHAR(30),
	jobquery VARCHAR(50),
	jobtitle VARCHAR(200),
	company VARCHAR(200),
	city VARCHAR(30),
	state VARCHAR(20),
	country VARCHAR(5),
	latitude FLOAT,
	longitude FLOAT,
	language VARCHAR(5),
	formattedlocation VARCHAR(30),
	jobsource VARCHAR(200),
	jobdate VARCHAR(40),
	url VARCHAR(300),
	onmousedown VARCHAR(30),
	sponsored BOOLEAN,
	expired BOOLEAN,
	indeedapply BOOLEAN,
	formattedlocationfull VARCHAR(50),
	formattedrelativetime VARCHAR(30),
	stations VARCHAR(30)
	)
""".format(STAGE_TABLE_NAME)
# </editor-fold>


class IndeedTransformer(BaseTransformer):

    def __init__(self, *args, **kwargs):
        super(IndeedTransformer, self).__init__(*args, **kwargs)
        self._stage_table_name = STAGE_TABLE_NAME
        self._stage_table_ddl = STAGE_TABLE_DDL
        self._tsv_fields = FIELDS

    def transform(self, doc):

        yield {
            'jobkey': doc.get('jobkey'),
            'jobquery': doc.get('query'),
            'jobtitle': doc.get('jobtitle'),
            'company': doc.get('company'),
            'city': doc.get('city'),
            'state': doc.get('state'),
            'country': doc.get('country'),
            'latitude': doc.get('latitude'),
            'longitude': doc.get('longitude'),
            'language': doc.get('language'),
            'formattedlocation': doc.get('formattedLocation'),
            'jobsource': doc.get('source'),
            'jobdate': doc.get('date'),
            'url': doc.get('url'),
            'onmousedown': doc.get('onmousedown'),
            'sponsored': doc.get('sponsored'),
            'expired': doc.get('expired'),
            'indeedapply': doc.get('indeedApply'),
            'formattedlocationfull': doc.get('formattedLocationFull'),
            'formattedrelativetime': doc.get('formattedRelativeTime'),
            'stations': doc.get('stations'),
        }