from sqlalchemy import create_engine, Column, Integer, String, Float, Text, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import config

Base = declarative_base()

# Таблица связи документов и слов (many-to-many)
document_term = Table('document_term', Base.metadata,
                      Column('document_id', Integer, ForeignKey('documents.id')),
                      Column('term_id', Integer, ForeignKey('terms.id'))
                      )


class Document(Base):
    __tablename__ = 'documents'

    id = Column(Integer, primary_key=True)
    url = Column(String(500), unique=True)
    title = Column(String(500))
    content = Column(Text)
    pagerank = Column(Float, default=1.0)

    # Связи
    terms = relationship("Term", secondary=document_term, back_populates="documents")
    outgoing_links = relationship("Link", foreign_keys="Link.source_id")
    incoming_links = relationship("Link", foreign_keys="Link.target_id")


class Term(Base):
    __tablename__ = 'terms'

    id = Column(Integer, primary_key=True)
    word = Column(String(100), unique=True, index=True)

    documents = relationship("Document", secondary=document_term, back_populates="terms")


class Link(Base):
    __tablename__ = 'links'

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey('documents.id'))
    target_id = Column(Integer, ForeignKey('documents.id'))

    source = relationship("Document", foreign_keys=[source_id])
    target = relationship("Document", foreign_keys=[target_id])


class Database:
    def __init__(self):
        self.engine = create_engine(f'sqlite:///{config.DB_PATH}')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def get_or_create_document(self, url, title, content):
        doc = self.session.query(Document).filter_by(url=url).first()
        if not doc:
            doc = Document(url=url, title=title, content=content)
            self.session.add(doc)
            self.session.commit()
        return doc

    def get_or_create_term(self, word):
        term = self.session.query(Term).filter_by(word=word).first()
        if not term:
            term = Term(word=word)
            self.session.add(term)
            self.session.commit()
        return term

    def add_link(self, source_doc, target_doc):
        link = self.session.query(Link).filter_by(
            source_id=source_doc.id, target_id=target_doc.id
        ).first()

        if not link:
            link = Link(source_id=source_doc.id, target_id=target_doc.id)
            self.session.add(link)
            self.session.commit()

        return link

    def get_all_documents(self):
        return self.session.query(Document).all()

    def get_all_links(self):
        return self.session.query(Link).all()

    def update_pagerank(self, doc_id, pagerank):
        doc = self.session.query(Document).get(doc_id)
        if doc:
            doc.pagerank = pagerank
            self.session.commit()

    def search_by_term(self, term_word):
        term = self.session.query(Term).filter_by(word=term_word).first()
        if term:
            return term.documents
        return []

    def close(self):
        self.session.close()