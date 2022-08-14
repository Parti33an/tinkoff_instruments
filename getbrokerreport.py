#Пример получения брокерского отчета по всем счетам
#ВНИМАНИЕ: GenerateBrokerReportRequest не позволяет получить отчет за интервал более 31 дня, нужный интервал набирать кусками

from mytoken import token
import asyncio
from datetime import datetime
from tinkoff.invest.constants import INVEST_GRPC_API
from tinkoff.invest import (
    AccessLevel,
    CandleInstrument,
    Client,
    AsyncClient,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
    GenerateBrokerReportRequest,
    GetBrokerReportRequest
)


async def main():
    async with AsyncClient(token.TOKEN, target=INVEST_GRPC_API) as client:
        counts = await client.users.get_accounts()
        counts = counts.accounts
        access_counts = []
        for count in counts:
            if not (count.access_level == AccessLevel.ACCOUNT_ACCESS_LEVEL_NO_ACCESS):
                print(f"Счет {count.name} доступен")
                access_counts.append(count)
            else:
                print(f"Счет {count.name} недоступен!")
        
        for count in access_counts:   
            request = GenerateBrokerReportRequest( account_id = count.id, from_= datetime(2022, 8, 1), to = datetime.now())
            report = await client.operations.get_broker_report(
                        generate_broker_report_request = request 
                                                )
            print(report.get_broker_report_response.broker_report)

if __name__ == "__main__":
    asyncio.run(main())