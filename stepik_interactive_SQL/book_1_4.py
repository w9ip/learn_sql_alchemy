from sqlalchemy.orm import declarative_base, Mapped, mapped_column, Session
from sqlalchemy import create_engine
from pprint import pprint

engine = create_engine("postgresql+psycopg2://w9i@localhost/w9i", echo=True)


Base = declarative_base()


class Book(Base):

    __tablename__ = "book"

    book_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column()
    author: Mapped[str] = mapped_column()
    price: Mapped[float] = mapped_column()
    amount: Mapped[int] = mapped_column()

    def __repr__(self) -> str:
        result = ("Book(book_id={}, title={}, author={}, price={}, amount={})")
        return result.format(self.book_id, self.title, self.author,
                             self.price, self.amount)


data = (
    ('Мастер и Маргарита', 'Булгаков М.А.', 670.99, 3),
    ('Белая гвардия', 'Булгаков М.А.', 540.50, 5),
    ('Идиот', 'Достоевский М.А.', 460.00, 10),
    ('Братья Карамазовы', 'Достоевский М.А.', 799.01, 3),
    ('Игрок', 'Достоевский М.А.', 480.50, 10),
    ('Стихотворение и поэмы', 'Есенин С.А.', 650.00, 15),
)


def inserter(data: tuple):
    for i in data:
        title, author, price, amount = i
        yield Book(title=title, author=author, price=price, amount=amount)


def create_current_tb():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add_all(inserter(data))
        session.commit()


if __name__ == '__main__':
    # create_current_tb()
    from sqlalchemy import any_, select, all_, func, text

    with engine.connect() as session:

        inner_stmt = all_(
            select(func.avg(Book.amount))
            .group_by(Book.author)
            .scalar_subquery()
        )
        subq = inner_stmt
        result = session.execute(
            select(Book)
            .where(Book.amount < subq)
        )

        scal = result.scalars().all()
        row = result.all()

        print('scal', scal)
        print('type(scal)', type(scal))
        print('type(scal[0])', type(scal[0]))

        print('row', row)
        print('type(row)', type(row))
        # print(type(row))
