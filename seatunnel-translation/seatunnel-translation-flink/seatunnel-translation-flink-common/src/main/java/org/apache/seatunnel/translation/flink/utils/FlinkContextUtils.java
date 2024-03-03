package org.apache.seatunnel.translation.flink.utils;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;

@Slf4j
public class FlinkContextUtils {

    public static StreamingRuntimeContext getStreamingRuntimeContext(
            SourceReaderContext readerContext) {
        try {
            Field field = readerContext.getClass().getDeclaredField("this$0");
            field.setAccessible(true);
            AbstractStreamOperator<?> operator =
                    (AbstractStreamOperator<?>) field.get(readerContext);
            return operator.getRuntimeContext();
        } catch (Exception e) {
            throw new IllegalStateException("Initialize flink context failed", e);
        }
    }

    public static StreamingRuntimeContext getStreamingRuntimeContextForV14(
            Sink.InitContext writerContext) {
        try {
            // In flink 1.14, it has contained runtimeContext in InitContext, so first step to
            // detect if
            // it is existed
            Field field = writerContext.getClass().getDeclaredField("runtimeContext");
            field.setAccessible(true);
            return (StreamingRuntimeContext) field.get(writerContext);
        } catch (Exception e) {
            return null;
        }
    }

    public static StreamingRuntimeContext getStreamingRuntimeContextForV15(
            Sink.InitContext writerContext) {
        try {
            Field contextImplField = writerContext.getClass().getDeclaredField("context");
            contextImplField.setAccessible(true);
            Object contextImpl = contextImplField.get(writerContext);
            Field runtimeContextField = contextImpl.getClass().getDeclaredField("runtimeContext");
            runtimeContextField.setAccessible(true);
            return (StreamingRuntimeContext) runtimeContextField.get(contextImpl);
        } catch (Exception e) {
            throw new IllegalStateException("Initialize flink context failed", e);
        }
    }

    public static String getJobIdForV15(SplitEnumeratorContext enumContext) {
        try {
            SourceCoordinatorContext coordinatorContext = (SourceCoordinatorContext) enumContext;
            Field field =
                    coordinatorContext.getClass().getDeclaredField("operatorCoordinatorContext");
            field.setAccessible(true);
            OperatorCoordinator.Context operatorCoordinatorContext =
                    (OperatorCoordinator.Context) field.get(coordinatorContext);
            Field[] fields = operatorCoordinatorContext.getClass().getDeclaredFields();
            Optional<Field> fieldOptional =
                    Arrays.stream(fields)
                            .filter(f -> f.getName().equals("globalFailureHandler"))
                            .findFirst();
            if (!fieldOptional.isPresent()) {
                // RecreateOnResetOperatorCoordinator.QuiesceableContext
                fieldOptional =
                        Arrays.stream(fields)
                                .filter(f -> f.getName().equals("context"))
                                .findFirst();
                field = fieldOptional.get();
                field.setAccessible(true);
                operatorCoordinatorContext =
                        (OperatorCoordinator.Context) field.get(operatorCoordinatorContext);
            }

            // OperatorCoordinatorHolder.LazyInitializedCoordinatorContext
            field =
                    Arrays.stream(operatorCoordinatorContext.getClass().getDeclaredFields())
                            .filter(f -> f.getName().equals("globalFailureHandler"))
                            .findFirst()
                            .get();
            field.setAccessible(true);

            // SchedulerBase$xxx
            Object obj = field.get(operatorCoordinatorContext);
            fields = obj.getClass().getDeclaredFields();
            field =
                    Arrays.stream(fields)
                            .filter(f -> f.getName().equals("arg$1"))
                            .findFirst()
                            .get();
            field.setAccessible(true);
            SchedulerBase schedulerBase = (SchedulerBase) field.get(obj);
            return schedulerBase.getExecutionGraph().getJobID().toString();
        } catch (Exception e) {
            throw new IllegalStateException("Initialize flink job-id failed", e);
        }
    }

    public static String getJobIdForV14(Sink.InitContext writerContext) {
        StreamingRuntimeContext runtimeContext = getStreamingRuntimeContextForV14(writerContext);
        return runtimeContext != null ? runtimeContext.getJobId().toString() : null;
    }

    public static String getJobIdForV15(Sink.InitContext writerContext) {
        return getStreamingRuntimeContextForV15(writerContext).getJobId().toString();
    }
}
