import pandera as pa
from pandera import DataFrameModel, Field


class PaperSchema(DataFrameModel):
    id: str = Field(nullable=False, unique=True)
    article_title: str = Field(nullable=False, unique=False)
    publish_title: str = Field(nullable=False, unique=False)
    abstract: str = Field(nullable=False, unique=False)
    publish_date: str = Field(nullable=False, unique=False)
    cited_by_count: int = Field(nullable=False, unique=False)
    reference_count: int = Field(nullable=False, unique=False)

    @pa.check("cited_by_count", name="cited_by_count_check")
    def cited_by_count_check(cls, cited_by_count: int) -> bool:
        return cited_by_count >= 0
