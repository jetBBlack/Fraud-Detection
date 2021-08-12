from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueState, ValueStateDescriptor
from model.server_log import ServerLog, fromString

class FraudDetection(KeyedProcessFunction):
    def __init__(self) -> None:
        self.__loginState = ValueState(Types.BOOLEAN)
        self.__prevLoginCountry = ValueState(Types.STRING)
        self.__timerState = ValueState(Types.LONG)

    def open(self, runtime_context: RuntimeContext):
        #get state of login
        loginDescriptor = ValueStateDescriptor("login-flag", Types.BOOLEAN)
        self.__loginState = runtime_context.get_state(loginDescriptor)

        #used to track state of previous country
        prevCountryDescriptor = ValueStateDescriptor("prev-country", Types.STRING)
        self.__prevLoginCountry = runtime_context.get_state(prevCountryDescriptor)

        #used to keep track of timer 
        timerStateDescriptor = ValueStateDescriptor('timer-state', Types.LONG)
        self.__timerState = runtime_context.get_state(timerStateDescriptor)

        return super().open(runtime_context)
    
    def process_element(self, value:str, ctx: 'KeyedProcessFunction.Context', collector):
        logEvent = fromString(value=value)
        isLoggedIn = self.__loginState.value
        prevCountry = self.__prevLoginCountry.value

        if (isLoggedIn != None and prevCountry != None):
            if (isLoggedIn == True and logEvent.eventType == 'login'):
                if (prevCountry!= logEvent.locationCountry):
                    alert = f"Alert EventID: {logEvent.eventId}"+f"violatingAccountId: {logEvent.accountId}, prevCountry: {prevCountry}, " + f"currentCountry: {logEvent.locationCountry}"
                    collector.collect(alert)

        elif (logEvent.eventType == "login"):
            # set login and set prev login country
            self.__loginState.update(True)
            self.__prevLoginCountry.update(logEvent.locationCountry)

            timer = logEvent.eventTimeSteamp + int(5 * 60 * 1000)
            ctx.timer_service().register_processing_time_timer(timer)
            self.__timerState.update(timer)
        
        if (logEvent.eventType == 'log-out'):
            self.__loginState.clear()
            self.__prevLoginCountry.clear()

            #remove timer
            timer = self.__timerState.value()
            if timer != None:
                ctx.timer_service().delete_processing_time_timer(timer)
            
            self.__timerState.clear()

        return super().process_element(value, ctx)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        self.__timerState.clear()
        self.__loginState.clear()
        self.__prevLoginCountry.clear()
        return super().on_timer(timestamp, ctx)

  

