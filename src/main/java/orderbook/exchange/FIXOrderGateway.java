package orderbook.exchange;

import orderbook.core.Order;
import orderbook.core.OrderBook;
import quickfix.*;
import quickfix.field.Side;
import quickfix.fix40.NewOrderSingle;

public class FIXOrderGateway implements Application {

    private final OrderBook orderBook;

    public FIXOrderGateway(OrderBook orderBook) {
        this.orderBook = orderBook;
    }

    @Override
    public void onCreate(SessionID sessionID) {}

    @Override
    public void onLogon(SessionID sessionID) {
        System.out.println("FIX session logged on: " + sessionID);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        if(message instanceof NewOrderSingle){
            NewOrderSingle nos = (NewOrderSingle) message;
            Order order = new Order(
                    nos.getClOrdID().getValue(),
                    nos.getSide().getValue() == Side.BUY,
                    nos.getPrice().getValue(),
                    (int)nos.getOrderQty().getValue(),
                    nos.getSymbol().getValue()
            );
            orderBook.processOrder(order);
        }
    }

    @Override
    public void onLogout(SessionID sessionID) {

    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {

    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {

    }

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {

    }


}
