from sqlalchemy import create_engine, Column, Integer, String, Boolean, select, Float, update, delete, bindparam, literal
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy_memory import MemorySession
import argparse
import time
import random
from faker import Faker


random.seed(42)
Base = declarative_base()
fake = Faker()
CATEGORIES = list("ABCDEFGHIJK")

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    active = Column(Boolean, index=True)
    category = Column(String, index=True)
    price = Column(Float, index=True)
    cost = Column(Float)

def generate_items(n):
    for _ in range(n):
        yield Item(
            name=fake.name(),
            active=random.choice([True, False]),
            category=random.choice(CATEGORIES),
            price=round(random.uniform(5, 500), 2),
            cost=round(random.uniform(1, 300), 2),
        )

def generate_random_select_query():
    clauses = []

    if random.random() < 0.5:
        val = random.choice([True, False])
        op = random.choice([operators.eq, operators.ne])
        clauses.append(BinaryExpression(Item.active, literal(val), op))

    if random.random() < 0.7:
        subset = random.sample(CATEGORIES, random.randint(1, 4))
        op = random.choice([operators.in_op, operators.notin_op])
        param = bindparam("category_list", subset, expanding=True)
        clauses.append(BinaryExpression(Item.category, param, op))

    if random.random() < 0.6:
        price_val = round(random.uniform(10, 400), 2)
        op = random.choice([operators.gt, operators.lt, operators.le, operators.gt])
        clauses.append(BinaryExpression(Item.price, literal(price_val), op))

    if random.random() < 0.3:
        cost_val = round(random.uniform(10, 200), 2)
        op = random.choice([operators.gt, operators.lt, operators.le, operators.gt])
        clauses.append(BinaryExpression(Item.cost, literal(cost_val), op))

    if not clauses:
        clauses.append(Item.active == True)

    return select(Item).where(*clauses)

def inserts(Session, count):
    insert_start = time.time()
    with Session() as session:
        session.add_all(generate_items(count))
        session.commit()
    insert_duration = time.time() - insert_start
    print(f"Inserted {count} items in {insert_duration:.2f} seconds.")
    return insert_duration

def selects(Session, count, fetch_type):
    queries = [generate_random_select_query() for _ in range(count)]

    query_start = time.time()
    with Session() as session:
        for stmt in queries:
            if fetch_type == "limit":
                stmt = stmt.limit(5)

            result = session.execute(stmt)

            if fetch_type == "first":
                result.first()
            else:
                list(result.scalars())

    query_duration = time.time() - query_start
    print(f"Executed {count} select queries ({fetch_type}) in {query_duration:.2f} seconds.")
    return query_duration

def updates(Session, random_ids):
    update_start = time.time()
    with Session() as session:
        for rid in random_ids:
            stmt = update(Item).where(Item.id == rid).values(
                name=fake.name(),
                category=random.choice(CATEGORIES),
                active=random.choice([True, False])
            )
            session.execute(stmt)
        session.commit()
    update_duration = time.time() - update_start
    print(f"Executed {len(random_ids)} updates in {update_duration:.2f} seconds.")
    return update_duration

def deletes(Session, random_ids):
    delete_start = time.time()
    with Session() as session:
        for rid in random_ids:
            stmt = delete(Item).where(Item.id == rid)
            session.execute(stmt)
        session.commit()
    delete_duration = time.time() - delete_start
    print(f"Deleted {len(random_ids)} items in {delete_duration:.2f} seconds.")
    return delete_duration

def run_benchmark(db_type="sqlite", count=100_000):
    print(f"Running benchmark: type={db_type}, count={count}")

    if db_type == "sqlite":
        engine = create_engine("sqlite:///:memory:", echo=False)
        Session = sessionmaker(engine)
    elif db_type == "memory":
        engine = create_engine("memory://")
        Session = sessionmaker(
            engine,
            class_=MemorySession,
            expire_on_commit=False,
        )
    else:
        raise ValueError("Invalid --type. Use 'sqlite' or 'memory'.")

    Base.metadata.create_all(engine)

    elapsed = inserts(Session, count)
    elapsed += selects(Session, 500, fetch_type="all")
    elapsed += selects(Session, 500, fetch_type="limit")

    random_ids = random.sample(range(1, count + 1), 500)
    elapsed += updates(Session, random_ids)

    random_ids = random.sample(range(1, count + 1), 500)
    elapsed += deletes(Session, random_ids)

    print(f"Total runtime for {db_type}: {elapsed:.2f} seconds.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", choices=["sqlite", "memory"], required=True)
    parser.add_argument("--count", type=int, default=10_000)
    args = parser.parse_args()
    run_benchmark(args.type, args.count)
