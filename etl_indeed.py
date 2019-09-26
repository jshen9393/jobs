
from etl.indeed.indeed_extractor import IndeedExtractor
from etl.indeed.indeed_transformer import IndeedTransformer
from etl.common.tsv_loader import TsvLoader

def main(*args, **kwargs):
    extractor = IndeedExtractor()
    transformer = IndeedTransformer()
    tsv_loader = TsvLoader(extractor, transformer)
    tsv_file = tsv_loader.load()

for row in extractor.extract():
    print (row)