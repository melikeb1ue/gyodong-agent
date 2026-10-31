import os
os.environ.setdefault('ANTHROPIC_API_KEY','offline-placeholder')
import asyncio,unittest,httpx
from unittest.mock import AsyncMock,patch
import app
class Checks(unittest.IsolatedAsyncioTestCase):
 async def test_webhook_validation_and_dedup(self):
  app.SEEN_CALLBACKS.clear()
  with patch.object(app,'handle',new=AsyncMock()) as h:
   async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app.app),base_url='http://local') as c:
    self.assertEqual((await c.get('/')).json()['tools'],40)
    for data in [[],None,{'op':0,'t':'MESSAGE_CREATE','d':[]}]:
     self.assertEqual((await c.post('/webhook',json=data)).status_code,204)
    d={'op':0,'t':'MESSAGE_CREATE','d':{'botUserKey':'a','callbackToken':'unique','content':'test'}}
    await c.post('/webhook',json=d);await c.post('/webhook',json=d);await asyncio.sleep(0)
   self.assertEqual(h.await_count,1)
 async def test_no_mutation_retry(self):
  name=next(n for n,op in app.OPS.items() if op[0]=='post' and op[1]=='/api/refunds')
  with patch.object(app.http,'request',new=AsyncMock(side_effect=httpx.ReadTimeout('timeout'))) as req:
   result=await app.call_action(name,{'body':{}},'L-47')
   self.assertEqual(req.await_count,1);self.assertEqual(result['status'],'network_error')
 async def test_timeout(self):
  async def slow(*_):await asyncio.sleep(1)
  with patch.object(app,'DEADLINE_SEC',0.01),patch.object(app,'run_agent',side_effect=slow),patch.object(app,'send_callback',new=AsyncMock()) as cb:
   await app.handle({'botUserKey':'timeout','callbackToken':'x','content':'test'})
   self.assertEqual(app.SESSIONS['timeout']['msgs'],[])
   self.assertIn('확정할 수 없습니다',cb.await_args.args[1])
 async def test_callback(self):
  with patch.object(app.http,'post',new=AsyncMock(return_value=httpx.Response(200))) as post:
   await app.send_callback('x','답변')
   self.assertEqual(post.await_args.kwargs['json']['skillResponse']['template']['outputs'],[{'simpleText':{'text':'답변'}}])
 def test_tools_knowledge_and_length(self):
  self.assertEqual(len(set(t['name'] for t in app.TOOLS)),40)
  self.assertFalse(any('reset' in t['name'] for t in app.TOOLS))
  self.assertLessEqual(len(app.shorten('가'*600)),480)
  self.assertEqual(app.extract_tc('TC: R-05-a 문의'),'R-05-a')
  for term in ['정상가 인하','가예약','TX-05','픽업','홍보 협약 계약서']:self.assertIn(term,app.KNOWLEDGE)
if __name__=='__main__':unittest.main()
