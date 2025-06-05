from typing import List, Optional
from pydantic import BaseModel, Field


class Image(BaseModel):
    # make every field optional – Hardcover sometimes returns {}
    url:    Optional[str] = None
    height: Optional[int] = None
    width:  Optional[int] = None
    color:       Optional[str] = None
    color_name:  Optional[str] = None

    class Config:                # <-- NESTED
        extra = "ignore"         # ignore any stray keys


class BookDoc(BaseModel):
    id: int
    title: str
    isbns: List[str]
    rating:         Optional[float] = None   # ? rename
    ratings_count:  Optional[int]   = None   # ? keep
    publication_date: date | None = None   

    class Config:
        extra = "allow"          # ignore fields we don’t list
