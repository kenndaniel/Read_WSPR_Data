


def findSpot(data, spotTime, timeSlot,myCall, stdTelemCall, telemCall1, telemCall2): 
    # Filter by callsign and time slot
    stdMsgTime = timeSlot-2
    if stdMsgTime < 0:
        stdMsgTime = 8
    # SendCall is defined in dbinterface
    if (data[SendCall]== myCall and spotTime.minute%10 == abs(stdMsgTime)):  
        # Find new standard message
        print(data)
        return True

    if ( data[SendCall][0]== telemCall1[0] and data[SendCall][1]== telemCall1[1] and data[SendCall][2]== telemCall1[2]
            and spotTime.minute%10 == (timeSlot + 2)%10): 
        # Watch for custom telementry calls 1 e.g. T19
        return True
    
    if ( data[SendCall][0]== telemCall2[0] and data[SendCall][1]== telemCall2[1] and data[SendCall][2]== telemCall2[2]
            and spotTime.minute%10 == (timeSlot + 4)%10): 
        # Watch for custom telementry call 2 e.g. T99
        return True
    
    if ( data[SendCall][0]== stdTelemCall[0] and data[SendCall][2]== stdTelemCall[1] 
        and spotTime.minute%10 == timeSlot): 
        # Watch for standard telementry calls
        return True
    
    # output bad data
    #print('********************************************')
    #print(spotTime)
    #print(data)
    return False