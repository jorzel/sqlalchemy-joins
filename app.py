from curses import meta
from sqlalchemy import event

from sqlalchemy import Column, MetaData, String, create_engine, Integer, ForeignKey
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    relationship,
    contains_eager,
    configure_mappers,
    joinedload,
)

DB_URI = "sqlite://"

engine = create_engine(DB_URI)
metadata = MetaData()
Base = declarative_base(metadata=metadata)

Session = sessionmaker(engine)
Session.configure(bind=engine)


class DBStatementCounter(object):
    """
    https://stackoverflow.com/questions/19073099/how-to-count-sqlalchemy-queries-in-unit-tests
    Use as a context manager to count the number of execute()'s performed
    against the given sqlalchemy connection.

    Usage:
        with DBStatementCounter(conn) as ctr:
            conn.execute("SELECT 1")
            conn.execute("SELECT 1")
        assert ctr.get_count() == 2
    """

    def __init__(self, conn):
        self.conn = conn
        self.count = 0
        # Will have to rely on this since sqlalchemy 0.8 does not support
        # removing event listeners
        self.do_count = False
        event.listen(conn, "after_execute", self.callback)

    def __enter__(self):
        self.do_count = True
        return self

    def __exit__(self, *_):
        self.do_count = False

    def get_count(self):
        return self.count

    def callback(self, *_):
        if self.do_count:
            self.count += 1



class Person(Base):
    __tablename__ = "person"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    user = relationship("User", uselist=False)


class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("person.id"))
    person = relationship("Person")
    my_accounts = relationship("UserAccount")


class Company(Base):
    __tablename__ = "company"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)


class Account(Base):
    __tablename__ = "account"
    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String)
    company_id = Column(Integer, ForeignKey("company.id"))
    company = relationship("Company")


class UserAccount(Base):
    __tablename__ = "user_account"
    account_id = Column(Integer, ForeignKey("account.id"), primary_key=True)
    account = relationship("Account")
    user_id = Column(Integer, ForeignKey("user.id"), primary_key=True)
    user = relationship("User")


def populate_db(n=10):
    users = []
    with Session() as session:
        for i in range(n):
            person = Person(name=f"test{i}")
            session.add(person)
            session.flush()
            user = User(person=person)
            session.add(user)
            session.flush()
            company = Company(name=f"company{i}")
            session.add(company)
            session.flush()
            account = Account(status="x{i}", company=company)
            session.add(account)
            session.flush()
            user_account = UserAccount(user=user, account=account)
            session.add(user_account)
            session.flush()
            session.commit()


configure_mappers()
metadata.drop_all(engine)
metadata.create_all(engine)
populate_db()


def get_query(session, options=None):
    if not options:
        options = []
    return (
        session.query(Person)
        .join("user", "my_accounts", "account", "company")
        .options(*options)
        .filter(
            Person.name.ilike("test%"),
            Account.status.ilike("x%"),
            Company.name.ilike("company%"),
        )
    )


def simple_query():
    print("--------")
    print("simple")
    with Session() as session:
        with DBStatementCounter(session.connection()) as ctr:
            result = get_query(session)
            print("Query")
            print(result)
            person = result.first()
            if person:
                a = person.user.my_accounts[0].account.company.name
        print("Statements")
        print(ctr.count)


def joinedload_query():
    print("--------")
    print("joinedload")
    with Session() as session:
        with DBStatementCounter(session.connection()) as ctr:
            result = get_query(
                session,
                options=[
                    joinedload("user"),
                    joinedload("user", "my_accounts"),
                    joinedload("user", "my_accounts", "account"),
                    joinedload("user", "my_accounts", "account", "company"),
                ],
            )
            print("Query")
            print(result)
            person = result.first()
            if person:
                a = person.user.my_accounts[0].account.company.name
        print("Statements")
        print(ctr.count)


def eager_query():
    print("--------")
    print("joinedload")
    with Session() as session:
        with DBStatementCounter(session.connection()) as ctr:
            result = get_query(
                session,
                options=[
                    contains_eager("user"),
                    contains_eager("user", "my_accounts"),
                    contains_eager("user", "my_accounts", "account"),
                    contains_eager("user", "my_accounts", "account", "company"),
                ],
            )
            print("Query")
            print(result)
            person = result.first()
            if person:
                a = person.user.my_accounts[0].account.company.name
        print("Statements")
        print(ctr.count)


simple_query()
joinedload_query()
eager_query()
