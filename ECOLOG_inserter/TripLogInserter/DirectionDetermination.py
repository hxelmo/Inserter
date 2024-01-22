#Tripの開始座標と終了座標がTommyHomeとoutならoutward,逆ならhomeward
from .Place_config import get_place
#from .Place_config import outStartLatitude, outEndLatitude, outStartLongitude, outEndLongitude, homeStartLatitude, homeEndLatitude, homeStartLongitude, homeEndLongitude
def DirectionDetermination(startLatitude,startLongitude,endLatitude,endLongitude,driver_id):
    outStartLatitude, outEndLatitude, outStartLongitude, outEndLongitude, homeStartLatitude, homeEndLatitude, homeStartLongitude, homeEndLongitude = get_place(driver_id)
    homeStart=False
    homeEnd=False
    outStart=False
    outEnd=False

    if (homeStartLatitude < startLatitude) & (startLatitude < homeEndLatitude) & (homeStartLongitude < startLongitude) & (startLongitude < homeEndLongitude):
        homeStart=True
    elif (outStartLatitude < startLatitude) & (startLatitude < outEndLatitude) & (outStartLongitude < startLongitude) & (startLongitude < outEndLongitude):
        outStart=True

    if (homeStartLatitude < endLatitude) & (endLatitude < homeEndLatitude) & (homeStartLongitude < endLongitude) & (endLongitude < homeEndLongitude):
        homeEnd=True
    elif (outStartLatitude < endLatitude) & (endLatitude < outEndLatitude) & (outStartLongitude < endLongitude) & (endLongitude < outEndLongitude):
        outEnd=True

    if(homeStart == True) & (outEnd == True):
        return 'outward'
    elif(outStart == True) & (homeEnd == True):
        return 'homeward'
    else:
        return 'others'

#print(DirectionDetermination(35.43153598,139.41400687,35.47217076,139.58674491,1))
#print(DirectionDetermination(35.472312175,139.58688214,35.43144774,139.41391006,1))
