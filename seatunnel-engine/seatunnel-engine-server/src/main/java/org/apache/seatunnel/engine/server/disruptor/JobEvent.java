package org.apache.seatunnel.engine.server.disruptor;

import org.apache.seatunnel.api.event.Event;

import com.lmax.disruptor.EventFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JobEvent {

    private Event event;

    public static final EventFactory<JobEvent> FACTORY = JobEvent::new;
}
