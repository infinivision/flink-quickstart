package org.infinivision.flink.rpc.dynamicproxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class DynamicProxy implements InvocationHandler, Person, Education {
    @Override
    public String getName() {
        return "Dynamic Proxy";
    }

    @Override
    public String school() {
        return "TJ";
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
        Class<?> declaringClass = method.getDeclaringClass();
        System.out.println("declaringClass: " + declaringClass);

        Object result = method.invoke(this, args);
        System.out.println("method call result: " + result);
        return result;
    }
}
