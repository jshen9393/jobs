
from etl.indeed.indeed_extractor import IndeedExtractor
from etl.indeed.indeed_transformer import IndeedTransformer
from etl.common.tsv_loader import TsvLoader
from etl.common.postgres_loader import PostGresLoader


def main(*args, **kwargs):
    print('starting indeed extract')
    extractor = IndeedExtractor()
    transformer = IndeedTransformer()
    tsv_loader = TsvLoader(extractor, transformer)
    tsv_loader.load()
    postgres_loader = PostGresLoader(transformer, tsv_loader)
    postgres_loader.rebuild_stage_table()
    postgres_loader.load()
    tsv_loader.cleanup()
    print('finished indeed extract')


if __name__ == '__main__':
    main()