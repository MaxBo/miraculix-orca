import orca
from orcadjango.decorators import meta
from extractiontools.destatis import Destatis


@meta(group='(7) Statistics', order=1)
@orca.step()
def search_tables(search_terms: str, database: str):
    """
    """
    destatis = Destatis(database, logger=orca.logger)
    for search_term in search_terms:
        destatis.add_table_codes(search_term)


@meta(group='(7) Statistics', order=2)
@orca.step()
def query_tables(destatis_tables: list, database: str,
                 project_area: 'ogr.Geometry', source_db: str):
    """
    """
    destatis = Destatis(database, logger=orca.logger)
    for choice in destatis_tables:
        code = choice.split(' | ')[0]
        destatis.download_table(code, project_area, source_db)