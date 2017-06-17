package pe.albatross.octavia.easydao;

import java.io.Serializable;
import java.util.List;

public interface EasyDAO<T extends Serializable> {

    T find(final long id);

    List<T> all();

    List<T> all(List<Long> ids);

    void save(final T entity);

    void update(final T entity);

    void delete(final T entity);

    void delete(final long id);

}
