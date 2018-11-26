package org.infinivision.flink.rpc.dynamicproxy;

import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;

public class DynamicClient {
    public static void main(String[] args) {
        ClassLoader classLoader = DynamicClient.class.getClassLoader();
        Class[] interfaces = new Class<?>[2];
        interfaces[0] = Person.class;
        interfaces[1] = Education.class;
        Object proxy = Proxy.newProxyInstance(classLoader, interfaces, new DynamicProxy());
        System.out.println("Proxy Class: " + proxy.getClass().getName());
        System.out.println("===constructor====");
        for(Constructor con : proxy.getClass().getConstructors()){
            System.out.println("name: " + con.getName());
            System.out.println("declaring class: " + con.getDeclaringClass());
            System.out.println(con.toGenericString());
        }

        System.out.println("===interfaces====");
        for(Class cl : proxy.getClass().getInterfaces()){
            System.out.println("name: " + cl.getName());
        }

        Person person = (Person)proxy;
        person.getName();

        Education education = (Education) proxy;
        education.school();
    }
}
