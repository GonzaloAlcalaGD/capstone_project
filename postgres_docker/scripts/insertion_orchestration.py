from insertion_factory import Factory
import data_generators as dg


if __name__ == '__main__':
    
    insertions = 10

    id = dg.generate_id()

    factory = Factory(id=id,
                      n_transactions=insertions, 
                      database='root',
                      user='root',
                      password='root',
                      host='localhost')


    status = factory.generate_insertions()
