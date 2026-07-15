import logging

import httpx
from httpx import HTTPStatusError

logger = logging.getLogger(__name__)


class MoynihanTrainHall(httpx.Client):
    def __init__(self):
        super().__init__(
            base_url="https://moynihantrainhall.nyc",
            timeout=30.0,
            headers={
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-language": "en-US,en;q=0.9,de-DE;q=0.8,de;q=0.7",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "dnt": "1",
                "origin": "https://moynihantrainhall.nyc",
                "priority": "u=1, i",
                "referer": "https://moynihantrainhall.nyc/transportation/",
                "sec-ch-ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "sec-gpc": "1",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
                "x-requested-with": "XMLHttpRequest",
            },
            cookies={
                "_ga": "GA1.1.1725338457.1784090609",
                "cf_clearance": "6I3GK_L0hRJ6RLyTriQL82zVvwgAmqiVJdhqoZ3Wb_Q-1784092527-1.2.1.1-HnaLiyjp8c5SlXQQ5xTnbKtEBKYwm1dM.LHkaTSCGzCVBQLRX6sIrVMx6ZfLkMZdGbUdDISwlpe92nx3K.O.Mtd1RaNljRFwO5izsm2ESGVBdb2fHDPToCW3Aa3fYVDcmjfQ1n1099dtiL4ttT3_hQk_ki2iroU3U6_93fGoyhAhTnOpSYniPw0CCw0Xn_cJIWZDPzBwGpwAGlj9FnYWf9.wwHK4pYjR8jkiYTUXNXKspeytMCkm60Ya1KYLkk.PVw7Uwap43ryihwrBrGP8dw9.5EgKWaGG0B4Ge5Q3DZw5CGpplx3VrC0zNhHUkLmK2srl_JOII2xh_sSxEfs.7w",
                "__cf_bm": "ph4FAoQOXyRNyK8Um2eRhF_xrOWhfcjcilx1SvzPx5I-1784092527.655643-1.0.1.1-azyqMD23QR4JGuF8HTxsYuJ2DUcWBRZsXnnZp2XsXNU6OZenJCMb6hAIp7heRCbbmt6wTedU7ycI7xM4jdPLADbW5K4EIKxvB.KJOkz7.Hyogt_R5jqbIfs.Wc6_xuTp",
                "_ga_EZWY6S93WR": "GS2.1.s1784090609$o1$g1$t1784092549$j38$l0$h0",
                "swpext86386": "1bc5bc897e2896a80f8d9ac10d2fdca2",
            },
            event_hooks={"response": [self._raise_for_status]},
        )

    def _raise_for_status(self, response: httpx.Response) -> None:
        try:
            response.raise_for_status()
        except HTTPStatusError:
            response.read()
            logger.error(response.request.url)
            logger.error(response.request.headers)
            logger.error(response.content)
            logger.error(response.headers)

    def get_train_board(self) -> dict[str, str]:
        res = self.post("/wp-admin/admin-ajax.php", data={"action": "ajax_amtrak_refresh"})
        return res.json()
