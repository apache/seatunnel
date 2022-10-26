# GoogleSheets

> GoogleSheets source connector

## Description

Used to read data from GoogleSheets.

## Options

| name                | type         | required | default value |
|-------------------  |--------------|----------|---------------|
| service_account_key | string       | yes      | -             |
| sheet_id            | string       | yes      | -             |
| sheet_name          | string       | yes      | -             |
| headers             | List<String> | yes      | -             |
| schema              | config       | yes      | -             |

### service_account_key [string]

google cloud service account, base64 required

### sheet_id [string]

sheet id in a Google Sheets URL

### sheet_name [string]

the name of the sheet you want to import

### headers [string]

as column name

### schema [Config]

#### fields [Config]

the schema fields of upstream data

## Example

simple:

```hocon
  GoogleSheets {
    service_account_key = "ewogIDJ0eXBlIjogInNlcnZpY2EfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAibW9vbi0zNjYzMDMiLAogICJwcml2YXRlX2tleV9pZCI6ICI3YjcxZjBhN2IxOTI4YTYxNWJhOWNiNjRlNDVkNzk0ZjI0YzIyZjczIiwKICAicHJpdmF0ZV9rZXkiOiAiLS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tXG5NSUlFdlFJQkFEQU5CZ2txaGtpRzl3MEJBUUVGQUFTQ0JLY3dnZ1NqQWdFQUFvSUJBUURuYzQwa0N5OS9vMHJlXG5TczV5SCtuSm05b3ZZTG9nUm1BeUdMU2I5KzFLWWVUM2JqRjk5VWdGSVc2UXR3MUl5Q3ZlRlBXaEkvK2VmMUJMXG5zVVN1WXA3VzdSckJDZEVsTzZJRVl0NklyMDR0aSt4eEtVa1hKa2x6dkY3RVh5VC9MbUdiWjZPZ3pURURSdDZ5XG5yZDhQOWVpR0M3eFQ2bnhpN05DNnd5OC9GZWpnRlBMS0p5U01ldU52M1k1OERINElWd0JkSkNPZ0l6cWFQcEhHXG54aUMzKzBzVCtpUmlnSEdUZWtlakZsenBWWnNuYzdpaENDV3UyNFZYNUpGVm1jQzVLMEhkVm1RWVliYmlRSUo2XG5sREZ2MHdpaWlFbDVpTW1pNkplT2lmalFCMjl5WU16WnNYWDhPVGY5R0xGbXNoQU83UUZrM1lDVGVnTDBjRjRKXG52cHV2c3hyMUFnTUJBQUVDZ2dFQURnUDc1cUVQa3EyMmlZV0RXbnNMMldpUFJHdVpyUDExLzh3Mnk2Y1V5NjIwXG50cy9XY1dlYWJrYTBtTGM1MFUvZmFjYkd2cWZwbXZ1c3JVdnUraSthUXk3TUY4RGFvZjJRcUs0ak9ham95TlRrXG42WUx6NlI3REJGRmNEOENQTmZwTHBBcm1BZXZMQndVcjFGNkVQbmo3ZTNxSzJmclhzM0JFOUZQbXNMcXZIenFPXG5idERKclMvRjlLd1ZJMnZDTHdET1REY3JuQTQrclA1c0Q2alIrSXZ0aHhaeFFxNjdLM0UyZWQ4ZDREem1rcnB5XG5teGJxWDIyK2EydUI2NHI2SzB1MHNBaTJrNlRmR1VwRVB3NWozM2JVOG1idktFOEZpcld6SjROWjJHUTZxVjNWXG5ESTlSV1Bud0taVEZYcm9UakgxRnlTa2Y2K1U2WjBmWnVNeW5Hd3NDRlFLQmdRRDR4WGFJeGtNanpjd2NsY2lDXG5YOWJiQWg1cVVjT2FNS2l4Uy9DL1FaN0tVcVFRU3UyRVVRSkV0V1lBVEU5OUNBWkZnZitaSkdaeTBQSGNpSWcxXG5ZdSs1a3hOSXlnMEpuQU9tOTArU0RiTXhXU25aQVI2d2NkSjFrbmRHcnE0TjZUeWNBR1dRR1A4dFU0V3FVd3YyXG5ic0JWS056NzVmcVVGYUlUY0ZOcXJGLy9od0tCZ1FEdUxVQUo5em43OU4yMXlzNHVZM1FnMmdTamRtMnhoRnk0XG5mbFpnN0RINXZneGdlUFFMVWtMV2NIN1RBdVRVNS9uOHpzUkl5WFdwcDY3dFJFVWhGeE55L25SRjBZVk9NY2RPXG5IcWxjUGRiWVJ4NXRBcE5YTElSSG1wd3BYaGl3eWR4cmhqb3d4Zi96ZnpYNVVWT3JRZXFadTFmbi9ZMnV3QnczXG5CbHZWQ0p0WW93S0JnUUR6ZnJ1bDVvR0RBd1Y5R1RLNFFieVlnenRvRDMzb1p5WkcyN0RhVXcxc3RYVDJxWkJRXG5DY2F4bitnbDlJdDlNeVNJVmxUcS9aRDFZd1RKT2k0d0t4Vy95M0I5dDU4SlA5Ym5UM3huMWRjR1AyTFJqK0lOXG5ER3ZXUHJWV3p0VjRza2xyM0l1TWZwc09MNGxPakpqUjdkZDl3MUNCa3Jvd3hmZnRGTzNoSkxXVjh3S0JnRnpxXG5uRVFIQmNEc2F4NERxMkhmZkt0Z1FsVCsyNVpZTzgvaHdaTDRNaHJxOEdzbVpKYVNGZThRWGZtMTlFMjIzZ2lwXG5KVnY3OUFJbW41Q0NoTEYvRGlCYmNrTFdsVG5ISTZpdlA0VHp2d0FLNXF6Q1IzYkhzQ2M3QXRjRWxYOUtrTW9GXG55R2ZFcHJCNG5UYlJhbUd5U25LRTZxdGZlRW1IT3JxZ3FXeTNlZHNmQW9HQVp0STlFV3ljVDA2cnpvOVMrK3hVXG41RGdrZnkyZUI1T052MmJGV1hGY1k1eHI1bUsxN0FOT0E0TnZRZlJLcHEzR1RvZzNKKzFIcXhKQnRNamdHNFIvXG5tV3c2amZtdFIzdWJiK0FnTWptKy9NbllWYzJrcncreHlDeStLMW5pZE4vMzF5ZTlrNk50dC9nSjhxNUt6ZFN2XG5RNnhVdXN4OWRkN3ZBZVdWYWhOUU9VQT1cbi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS1cbiIsCiAgImNsaWVudF9lbWFpbCI6ICJ0ZXN0YXBpQG1vb24tMzY2MzAzLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjExNzQ1MDI0NDQ4ODY1NzcyNDg4MiIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvdGVzdGFwaSU0MG1vb24tMzY2MzAzLmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9Cg=="
    sheet_id = "1VI0DvyZK-NIdssSdsDSsSSSC-_-rYMi7ppJiI_jhE"
    sheet_name = "sheets01"
    range = "A1:C3"
    headers = ["a", "b", "c"]
    schema = {
      fields {
        a: int
        b: string
        c: string
      }
    }
  }
```
