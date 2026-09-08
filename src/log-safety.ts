export function safeError(error:unknown):string {
  let message=error instanceof Error?error.message:String(error);
  for(const name of ['MOTHER_DUCK_API_KEY','LEPASS_API_KEY','LEPASS_ENCRYPTION_KEY','TRANSIT_SUMMARY_TOKEN']){
    const value=process.env[name];if(value)message=message.replaceAll(value,'[redacted]');
  }
  return message.replace(/(motherduck_token=)[^\s&'"`]+/gi,'$1[redacted]');
}
