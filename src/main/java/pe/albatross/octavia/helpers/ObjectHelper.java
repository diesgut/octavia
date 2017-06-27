package pe.albatross.octavia.helpers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectHelper {

    private static final Logger logger = LoggerFactory.getLogger(ObjectHelper.class);

    public static Object getParent(Object obj, String atributo) {
        Object parent = null;
        Method metodo = null;
        StringBuilder sbMetodo = new StringBuilder("get");
        sbMetodo.append(atributo.substring(0, 1).toUpperCase());
        sbMetodo.append(atributo.substring(1));

        for (Method metodoTmp : obj.getClass().getMethods()) {
            if (sbMetodo.toString().equals(metodoTmp.getName())) {
                metodo = metodoTmp;
                break;
            }
        }
        try {
            parent = metodo.invoke(obj);
        } catch (InvocationTargetException ex) {
        } catch (Exception ex) {
            logger.error("InvocationTargetException ::: " + ex.getLocalizedMessage());
        }

        return parent;
    }

    public static Object getParentTree(Object obj, String atributo) {
        Object objPadre = null;
        Object objHijo = obj;
        String[] attrs = atributo.split("\\.");

        for (String attr : attrs) {
            objPadre = getParent(objHijo, attr);
            if (objPadre == null) {
                return null;
            }
            objHijo = objPadre;
        }

        return objPadre;
    }

}
