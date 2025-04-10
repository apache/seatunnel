//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

@Private
@Unstable
public final class CleanerUtil {
    public static final boolean UNMAP_SUPPORTED;
    public static final String UNMAP_NOT_SUPPORTED_REASON;
    private static final BufferCleaner CLEANER;

    private CleanerUtil() {}

    public static BufferCleaner getCleaner() {
        return CLEANER;
    }

    private static Object unmapHackImpl() {
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        try {
            try {
                Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                MethodHandle unmapper =
                        lookup.findVirtual(
                                unsafeClass,
                                "invokeCleaner",
                                MethodType.methodType(Void.TYPE, ByteBuffer.class));
                Field f = unsafeClass.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                Object theUnsafe = f.get((Object) null);
                return newBufferCleaner(ByteBuffer.class, unmapper.bindTo(theUnsafe));
            } catch (SecurityException var10) {
                throw var10;
            } catch (RuntimeException | ReflectiveOperationException var11) {
                Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
                Method m = directBufferClass.getMethod("cleaner");
                m.setAccessible(true);
                MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
                Class<?> cleanerClass = directBufferCleanerMethod.type().returnType();
                MethodHandle cleanMethod =
                        lookup.findVirtual(cleanerClass, "clean", MethodType.methodType(Void.TYPE));
                MethodHandle nonNullTest =
                        lookup.findStatic(
                                        Objects.class,
                                        "nonNull",
                                        MethodType.methodType(Boolean.TYPE, Object.class))
                                .asType(MethodType.methodType(Boolean.TYPE, cleanerClass));
                MethodHandle noop =
                        MethodHandles.dropArguments(
                                MethodHandles.constant(Void.class, (Object) null)
                                        .asType(MethodType.methodType(Void.TYPE)),
                                0,
                                new Class[] {cleanerClass});
                MethodHandle unmapper =
                        MethodHandles.filterReturnValue(
                                        directBufferCleanerMethod,
                                        MethodHandles.guardWithTest(nonNullTest, cleanMethod, noop))
                                .asType(MethodType.methodType(Void.TYPE, ByteBuffer.class));
                return newBufferCleaner(directBufferClass, unmapper);
            }
        } catch (SecurityException var12) {
            return "Unmapping is not supported, because not all required permissions are given to the Hadoop JAR file: "
                    + var12
                    + " [Please grant at least the following permissions: RuntimePermission(\"accessClassInPackage.sun.misc\")  and ReflectPermission(\"suppressAccessChecks\")]";
        } catch (RuntimeException | ReflectiveOperationException var13) {
            return "Unmapping is not supported on this platform, because internal Java APIs are not compatible with this Hadoop version: "
                    + var13;
        }
    }

    private static BufferCleaner newBufferCleaner(
            Class<?> unmappableBufferClass, MethodHandle unmapper) {
        assert Objects.equals(MethodType.methodType(Void.TYPE, ByteBuffer.class), unmapper.type());

        return (buffer) -> {
            if (!buffer.isDirect()) {
                throw new IllegalArgumentException("unmapping only works with direct buffers");
            } else if (!unmappableBufferClass.isInstance(buffer)) {
                throw new IllegalArgumentException(
                        "buffer is not an instance of " + unmappableBufferClass.getName());
            } else {
                Throwable error =
                        (Throwable)
                                AccessController.doPrivileged(
                                        (PrivilegedAction)
                                                () -> {
                                                    try {
                                                        unmapper.invokeExact(buffer);
                                                        return null;
                                                    } catch (Throwable var3) {
                                                        return var3;
                                                    }
                                                });
                if (error != null) {
                    throw new IOException("Unable to unmap the mapped buffer", error);
                }
            }
        };
    }

    static {
        Object hack = AccessController.doPrivileged((PrivilegedAction) CleanerUtil::unmapHackImpl);
        if (hack instanceof BufferCleaner) {
            CLEANER = (BufferCleaner) hack;
            UNMAP_SUPPORTED = true;
            UNMAP_NOT_SUPPORTED_REASON = null;
        } else {
            CLEANER = null;
            UNMAP_SUPPORTED = false;
            UNMAP_NOT_SUPPORTED_REASON = hack.toString();
        }
    }

    @FunctionalInterface
    public interface BufferCleaner {
        void freeBuffer(ByteBuffer var1) throws IOException;
    }
}
