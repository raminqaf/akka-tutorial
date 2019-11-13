package de.hpi.ddm.actors;

import akka.actor.ActorRef;
import akka.stream.SourceRef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceRefMessage implements Serializable {
    private static final long serialVersionUID = 4057807743872319842L;
    private SourceRef<List<Byte>> sourceRef;
    private int length;
    private ActorRef sender;
    private ActorRef receiver;
}
