package ch.usi.dslab.lel.dynastarv2;

import ch.usi.dslab.lel.dynastarv2.command.Command;

public class Execution implements Runnable {
    private Command command;

    public Execution(Command command) {
        this.command = command;
    }

    @Override
    public void run() {

    }
}
