from sqlalchemy.orm import declarative_base, mapped_column, Mapped

Base = declarative_base()

class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Item(id={self.id} name={self.name})"
